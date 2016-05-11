/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import distributed_fs.exception.DFSException;
import distributed_fs.utils.Utils;
import distributed_fs.utils.VersioningUtils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Versioned;

public class DFSDatabase
{
	private final FileManagerThread _fileMgr;
	private final ScanDBThread scanDBThread;
	private CheckHintedHandoffDatabase hhThread;
	private final VersioningDatabase vDatabase;
	private boolean shutDown = false;
	private NavigableMap<ByteBuffer, DistributedFile> database;
	private String root;
	
	public static final Logger LOGGER = Logger.getLogger( DFSDatabase.class );
	
	/**
	 * Construct a new Distributed File System database.
	 * 
	 * @param resourcesLocation		location of the file. If {@code null}, will be set
	 * 								the default one ({@link Utils#RESOURCE_LOCATION}).
	 * @param databaseLocation		location of the database. If {@code null}, will be set
	 * 								the default one ({@link VersioningDatabase#DB_LOCATION}).
	 * @param fileMgr				the manager used to send/receive files.
	 *								If {@code null} the database for the hinted handoff
	 * 								nodes does not start.
	*/
	public DFSDatabase( final String resourcesLocation,
						final String databaseLocation,
						final FileManagerThread fileMgr ) throws IOException, DFSException
	{
		_fileMgr = fileMgr;
		if(_fileMgr != null) {
			hhThread = new CheckHintedHandoffDatabase();
			hhThread.start();
		}
		
		try{ vDatabase = new VersioningDatabase( databaseLocation ); }
		catch( Exception e ) {
			e.printStackTrace();
			throw new IOException();
		}
		
		database = new TreeMap<>();
		
		// Check if the path is written in the standard format.
		root = (resourcesLocation != null) ? resourcesLocation : Utils.RESOURCE_LOCATION;
		if(root.startsWith( "./" ))
			root = root.substring( 2 );
		
		File f = new File( root );
		root = f.getAbsolutePath();
		if(f.isDirectory() && !root.endsWith( "/" ))
			root += "/";
		root = root.replace( "\\", "/" ); // System parametric among Windows, Linux and MacOS
		
		if(!Utils.createDirectory( root )) {
			throw new DFSException( "Invalid database path " + root + "." +
									"Make sure that the path is correct and that you have the permissions to create and execute it." );
		}
		
		loadFiles( new File( root )/*, _fileMgr != null*/ );
		
		scanDBThread = new ScanDBThread( this );
		scanDBThread.start();
	}
	
	/**
	 * Load recursively the files present in the current folder.
	 * 
	 * @param dir	the current directory
	*/
	private void loadFiles( final File dir/*, final boolean makeSignature*/ ) throws IOException
	{
		for(File f : dir.listFiles()) {
			String fileName = f.getPath();
			if(f.isDirectory()) {
				loadFiles( f/*, makeSignature*/ );
				fileName += "/";
			}
			fileName = fileName.substring( root.length() );
			
			ByteBuffer fileId = Utils.getId( fileName );
			//LOGGER.debug( "File: " + fileName + ", Directory: " + f.isDirectory() + ", Id: " + Utils.bytesToHex( fileId.array() ) );
			
			VectorClock clock = null;
			boolean deleted = false;
			String hintedHandoff = null;
			
			try{
				// get the version from the versioning database
				String statement = "SELECT vClock, hintedHandoff, Deleted FROM versions " +
									"WHERE fileId = '" + Utils.bytesToHex( fileId.array() ) + "'";
				List<String> values = vDatabase.query( statement, 0 );
				if(values.size() > 0) {
					clock = Utils.deserializeObject( Utils.hexToBytes( values.get( 0 ) ).array() );
					hintedHandoff = values.get( 1 );
					if(hintedHandoff.length() == 0) hintedHandoff = null;
					deleted = values.get( 2 ).equals( "1" ) ? true : false;
				}
			}
			catch( SQLException e ) {
				e.printStackTrace();
			}
			
			if(clock == null) {
				clock = new VectorClock();
				// Save the version on its database, since it's not present.
				try { saveVersion( Utils.bytesToHex( fileId.array() ), Utils.serializeObject( clock ), "", deleted, 0, false ); }
				catch( SQLException e ) { e.printStackTrace(); }
			}
			
			//DistributedFile file = new DistributedFile( fileName, root, clock, makeSignature );
			DistributedFile file = new DistributedFile( fileName, root, clock );
			LOGGER.debug( "LOADED: " + file );
			file.setDeleted( deleted );
			if(hintedHandoff != null && hhThread != null)
				hhThread.saveFile( hintedHandoff, file );
			
			database.put( fileId, file );
		}
	}
	
	/**
	 * Save the version of a file in the versioning database.
	 * 
	 * @param fileId		the file identifier
	 * @param clock			the associated clock
	 * @param hintedHandoff	the hinted handoff address in the form {@code ipAddress:port}
	 * @param deleted		decide whether the file has been deleted
	 * @param TTL			Time To Live of the file
	 * @param update		{@code true} if the clock replace an old value,
	 * 						{@code false} otherwise
	*/
	private void saveVersion( final String fileId, final byte[] clock,
							  String hintedHandoff, final boolean deleted,
							  final int TTL, final boolean update ) throws SQLException
	{
		if(hintedHandoff == null)
			hintedHandoff = "";
		
		String statement;
		if(update) {
			statement = "UPDATE Versions SET " +
						"vClock = '" + Utils.bytesToHex( clock ) + "', " +
						"hintedHandoff = '" + hintedHandoff + "', " +
						"Deleted = '" + (deleted ? 1 : 0) + "', " + 
						"TTL = '" + TTL + "' " +
						"WHERE fileId = '" + fileId + "'";
		}
		else {
			statement = "INSERT INTO Versions (fileId, vClock, hintedHandoff, Deleted, TTL) VALUES ('" +
						fileId + "', " + "'" +
						Utils.bytesToHex( clock ) + "', '" +
						hintedHandoff + "', " +
						(deleted ? 1 : 0) + ", " +
						TTL + ")";
		}
		
		vDatabase.update( statement );
	}
	
	/** 
	 * Save a file on the database.
	 * 
	 * @param file				name of the file to save
	 * @param clock				the associated vector clock
	 * @param hintedHandoff		the hinted handoff address in the form {@code ipAddress:port}
	 * @param saveOnDisk		{@code true} if the file has to be saved on disk,
	 * 							{@code false} otherwise
	 * 
	 * @return the new clock, if updated, {@code null} otherwise
	*/
	public VectorClock saveFile( final RemoteFile file, final VectorClock clock,
								 final String hintedHandoff, final boolean saveOnDisk ) throws IOException, SQLException
	{
		return saveFile( file.getName(), file.getContent(),
						 clock, hintedHandoff,
						 saveOnDisk );
	}
	
	/** 
	 * Save a file on the database.
	 * 
	 * @param fileName		name of the file to save
	 * @param content		file's content
	 * @param clock			the associated vector clock
	 * @param hintedHandoff	the hinted handoff address in the form {@code ipAddress:port}
	 * @param saveOnDisk	{@code true} if the file has to be saved on disk,
	 * 						{@code false} otherwise
	 * 
	 * @return the new clock, if updated, {@code null} otherwise.
	*/
	public synchronized VectorClock saveFile( final String fileName, final byte[] content,
											  final VectorClock clock, final String hintedHandoff,
											  final boolean saveOnDisk ) throws IOException, SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.get( fileId );
		
		LOGGER.debug( "OLD FILE: " + file );
		
		VectorClock updated = null;
		
		if(file != null) {
			// Resolve the (possible) inconsistency through the versions.
			//System.out.println( "MY_CLOCK: " + file.getVersion() + ", IN_CLOCK: " + clock );
			if(!resolveVersions( file.getVersion(), clock )) {
				// The input version is newer than mine, then
				// it will override the current one.
				file.setVersion( clock );
				if(file.isDeleted())
					file.setDeleted( false );
				
				updated = clock;
				
				if(saveOnDisk) {
					saveVersion( Utils.bytesToHex( fileId.array() ),
								 Utils.serializeObject( file.getVersion() ),
								 hintedHandoff, false, 0, true );
					Utils.saveFileOnDisk( root + fileName, content );
				}
				
				database.put( fileId, file );
				if(hintedHandoff != null)
					hhThread.saveFile( hintedHandoff, file );
			}
		}
		else {
			updated = clock;
			
			if(saveOnDisk) {
				saveVersion( Utils.bytesToHex( fileId.array() ),
							 Utils.serializeObject( clock ),
							 hintedHandoff, false, 0, false );
				Utils.saveFileOnDisk( root + fileName, content );
			}
			
			//file = new DistributedFile( fileName, root, clock, _fileMgr != null );
			file = new DistributedFile( fileName, root, clock );
			database.put( fileId, file );
			if(hintedHandoff != null)
				hhThread.saveFile( hintedHandoff, file );
		}
		
		System.out.println( "UPDATED: " + updated );
		
		return updated;
	}
	
	/** 
	 * Removes a file on database and on disk.
	 * 
	 * @param fileName			name of the file to remove
	 * @param clock				actual version of the file
	 * @param compareVersions	
	 * 
	 * @return the new clock, if updated, {@code null} otherwise.
	*/
	public synchronized VectorClock removeFile( final String fileName,
												final VectorClock clock,
												final boolean compareVersions ) throws SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.get( fileId );
		VectorClock updated = null;
		
		//System.out.println( "CLOCK: " + clock + ", MY_CLOCK: " + file.getVersion() );
		
		// Check whether the input version is newer than mine.
		if(file == null || !resolveVersions( file.getVersion(), clock )) {
			if(file == null)
				file = new DistributedFile( fileName, root, clock );
			updated = clock;
			doRemove( file, fileId, clock );
		}
		
		return updated;
	}
	
	private void doRemove( final DistributedFile file, final ByteBuffer fileId, final VectorClock clock ) throws SQLException
	{
		file.setDeleted( true );
		file.setVersion( clock );
		
		saveVersion( Utils.bytesToHex( fileId.array() ),
					 Utils.serializeObject( clock ),
					 null, true, file.getTimeToLive(), true );
		
		Utils.deleteFileOnDisk( root + file.getName() );
	}

	/**
	 * Resolve the (possible) inconsistency through the versions.
	 * 
	 * @param myClock	the current vector clock
	 * @param vClock	the input vector clock
	 * 
	 * @return {@code true} if the current file is new newest one,
	 * 		   {@code false} otherwise.
	*/
	private boolean resolveVersions( final VectorClock myClock, final VectorClock vClock )
	{
		List<Versioned<Integer>> versions = new ArrayList<>();
		versions.add( new Versioned<Integer>( 0, myClock ) );
		versions.add( new Versioned<Integer>( 1, vClock ) );
		
		// get the list of concurrent versions
		//VectorClockInconsistencyResolver<Integer> vecResolver = new VectorClockInconsistencyResolver<>();
		//List<Versioned<Integer>> inconsistency = vecResolver.resolveConflicts( versions );
		List<Versioned<Integer>> inconsistency = VersioningUtils.resolveVersions( versions );
		System.out.println( "VERSIONS: " + inconsistency );
		
		if(inconsistency.size() == 1)
			return inconsistency.get( 0 ).getValue() == 0;
		
		// resolve the conflicts, using a time-based resolver
		TimeBasedInconsistencyResolver<Integer> resolver = new TimeBasedInconsistencyResolver<>();
		int id = resolver.resolveConflicts( inconsistency ).get( 0 ).getValue();
		
		return id == 0;
	}
	
	/** 
	 * Checks if a key is contained in the database.
	 * 
	 * @param fileId	file identifier
	 * 
	 * @return TRUE if the file is contained, FALSE otherwise
	*/
	public boolean containsKey( final ByteBuffer fileId )
	{
		return database.containsKey( fileId );
	}
	
	/** 
	 * Returns the keys in the range specified by the identifiers.
	 * 
	 * @param fromId	source node identifier
	 * @param destId	destination node identifier
	 * 
	 * @return The list of keys. It can be null if one of the input id is null.
	*/
	public synchronized List<DistributedFile> getKeysInRange( final ByteBuffer fromId, final ByteBuffer destId )
	{
		if(fromId == null || destId == null)
			return null;
		
		List<DistributedFile> result = new ArrayList<>();
		
		if(destId.compareTo( fromId ) >= 0)
			result.addAll( database.subMap( fromId, false, destId, true ).values() );
		else {
			result.addAll( database.tailMap( fromId, false ).values() );
			result.addAll( database.headMap( destId, true ).values() );
		}
		
		return result;
	}
	
	/**
	 * Returns the file specified by the given file name.
	 * 
	 * @param fileName	
	*/
	public DistributedFile getFile( final String fileName )
	{
		return getFile( Utils.getId( fileName ) );
	}
	
	/**
	 * Returns the file specified by the given id.
	 * 
	 * @param id	
	*/
	public synchronized DistributedFile getFile( final ByteBuffer id )
	{
		return database.get( id );
	}
	
	/**
	 * Returns all the stored files.
	 * 
	 * @return list containing all the files stored in the database
	*/
	public synchronized List<DistributedFile> getAllFiles()
	{
		return new ArrayList<>( database.values() );
	}
	
	/**
	 * Checks the existence of a file in the file system,
	 * starting from its root.
	 * 
	 * @param filePath	the file to search
	 * 
	 * @return {@code true} if the file is present,
	 * 		   {@code false} otherwise
	*/
	public boolean checkExistFile( final String filePath ) throws IOException
	{
		return checkInFileSystemExists( new File( root ), filePath );
	}
	
	private boolean checkInFileSystemExists( final File filePath, final String fileName )
	{
		File[] files = filePath.listFiles();
		if(files != null) {
			for(File file : files) {
				if(file.getPath().equals( fileName ))
					return true;
				
				if(file.isDirectory()) {
					if(checkInFileSystemExists( file, fileName ))
						return true;
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Returns the file system root location.
	*/
	public String getFileSystemRoot()
	{
		return root;
	}
	
	/**
	 * Checks if a member is a hinted handoff replica node.
	 * 
	 * @param nodeAddress	address of the node
	*/
	public void checkMember( final String nodeAddress )
	{
		hhThread.checkMember( nodeAddress );
	}

	public void shutdown()
	{
		vDatabase.shutdown();
		shutDown = true;
		
		scanDBThread.shutDown();
		try { scanDBThread.join(); }
		catch( InterruptedException e ) {}
		
		if(hhThread != null) {
			hhThread.shutDown();
			try { hhThread.join(); }
			catch( InterruptedException e ) {}
		}
		
		database.clear();
	}
	
	/**
	 * Class used to periodically test
	 * if a file has to be removed from the database.
	*/
	private class ScanDBThread extends Thread
	{
		private boolean shutDown = false;
		private final DFSDatabase database;
		
		// Time to wait before to check the database (1 minute).
		private static final int CHECK_TIMER = 60000;
		
		public ScanDBThread( final DFSDatabase database )
		{
			this.database = database;
		}
		
		@Override
		public void run()
		{
			while(!shutDown) {
				try{ Thread.sleep( CHECK_TIMER ); }
				catch( InterruptedException e ){ break; }
				
				List<DistributedFile> files = database.getAllFiles();
				for(int i = files.size() - 1; i >= 0; i--) {
					DistributedFile file = files.get( i );
					if(file.isDeleted()) {
						if(file.checkDelete()) {
							try { database.removeFile( file.getName(), file.getVersion(), false ); }
							catch( SQLException e ) {}
						}
						else {
							// Update the Time To Live of the file.
							try{
								saveVersion( Utils.bytesToHex( Utils.getId( file.getName() ).array() ),
											 Utils.serializeObject( file.getVersion() ),
											 file.getHintedHandoff(), true, file.getTimeToLive(), true );
							}
						 	catch( SQLException e ) {
						 		// Ignored.
						 		//e.printStackTrace();
						 	}
						}
					}
				}
			}
		}
		
		public void shutDown()
		{
			shutDown = true;
			interrupt();
		}
	}
	
	/**
	 * Class used to periodically test
	 * if a file has to be transmitted to
	 * its "real" owner.
	*/
	private class CheckHintedHandoffDatabase extends Thread
	{
		private final List<String> upNodes;
		/** Database containing the hinted handoff files; it manages objects of (dest. Ip address:port, [list of files]) */
		private final Map<String, List<DistributedFile>> hhDatabase;
		
		private static final int CHECK_TIMER = 5000;
		
		public CheckHintedHandoffDatabase()
		{
			upNodes = new LinkedList<String>();
			hhDatabase = new HashMap<String, List<DistributedFile>>();
		}
		
		@Override
		public void run()
		{
			while(!shutDown) {
				try{ Thread.sleep( CHECK_TIMER ); }
				catch( InterruptedException e ){
					break;
				}
				
				//LOGGER.debug( "HH_DATABASE: " + hhDatabase.size() + ", FILES: " + hhDatabase );
				
				for(String address : upNodes) {
					List<DistributedFile> files = hhDatabase.get( address );
					for(int i = files.size() - 1; i >= 0; i --) {
						if(files.get( i ).checkDelete()) {
							try { removeFile( files.get( i ).getName(), files.get( i ).getVersion(), false ); }
							catch ( SQLException e ) {}
							files.remove( i );
						}
					}
					
					// retrieve the informations from the saved address
					String[] data = address.split( ":" );
					String host = data[0];
					int port = Integer.parseInt( data[1] ) + 1;
					
					if(_fileMgr.sendFiles( port/*, Message.PUT*/, files, host, true, null, null ) ) {
						for(DistributedFile file : files) {
							String name = file.getName();
							try { removeFile( name, file.getVersion(), false ); }
							catch ( SQLException e ) {}
						}
						
						hhDatabase.remove( address );
					}
				}
			}
		}
		
		/**
		 * Saves a file for a hinted handoff replica node.
		 * 
		 * @param hintedHandoff		the node to which the file has to be sent, in the form {@code ipAddress:port}
		 * @param file				the corresponding file
		*/
		public void saveFile( final String hintedHandoff, final DistributedFile file )
		{
			file.setHintedHandoff( hintedHandoff );
			List<DistributedFile> files = hhDatabase.get( hintedHandoff );
			if(files == null) files = new ArrayList<>();
			files.add( file );
			hhDatabase.put( hintedHandoff, files );
		}
		
		/**
		 * Checks if a member is a hinted handoff replica node.
		 * 
		 * @param nodeAddress	address of the node
		*/
		public void checkMember( final String nodeAddress )
		{
			if(hhDatabase.containsKey( nodeAddress ))
				upNodes.add( nodeAddress );
		}
		
		public void shutDown()
		{
			interrupt();
		}
	}
	
	/**
	 * Class used to manage the read/write operations
	 * into the versioning database.<br>
	 * Each operation is lock-free, hence each of them
	 * must be called from a thread-safe method.
	*/
	private static class VersioningDatabase
	{
		private final Connection conn;
		private final String root;
		
		public static final String DB_LOCATION = "Database/DFSdatabase";
		
		public VersioningDatabase( final String dbFileName ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
		{
			Class.forName( "org.hsqldb.jdbcDriver" ).newInstance();
			
			Utils.createDirectory( root = (dbFileName != null) ? dbFileName : DB_LOCATION );
			
			// Connect to the database. This will load the db files and start the
			// database if it is not alread running.
			// db_file_name_prefix is used to open or create files that hold the state
			// of the db.
			// It can contain directory names relative to the
			// current working directory.
			// The password can be changed when the database is created for the first time.
			conn = DriverManager.getConnection( "jdbc:hsqldb:" + root, "cecco", "ste" );
			
			try {
				// make an empty table
				// by declaring the column IDENTITY, the db will automatically
				// generate unique values for new rows - useful for row keys
				update( "CREATE TABLE Versions ( id INTEGER IDENTITY, "
												+ "fileId VARCHAR(256), "
												+ "vClock VARCHAR(1024), "
												+ "hintedHandoff VARCHAR(256), "
												+ "Deleted INTEGER, "
												+ "TTL INTEGER )" );
			} catch( SQLException ex2 ) {
				// Ignore.
				//ex2.printStackTrace();  // Second time we run program
										  // should throw exception since table
										  // already there.
										  // This will have no effect on the db.
			}
		}
		
		/**
		 * Used for SQL commands CREATE, DROP, INSERT and UPDATE.
		 * 
		 * @param expression	the query
		*/
		public void update( final String expression ) throws SQLException
		{
			Statement st = conn.createStatement();
			
			if(st.executeUpdate( expression ) == -1)
				System.out.println( "Database error : " + expression );
			
			st.close();
		}
		
		/**
		 * Used for SQL command SELECT.
		 * 
		 * @param expression	the query
		 * @param columnIndex	column from where the value is retrieved
		*/
		public List<String> query( final String expression, final int columnIndex ) throws SQLException
		{
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery( expression );
			
			List<String> values = getValues( rs, columnIndex );
			st.close();    // NOTE!! if you close a statement the associated ResultSet is
						   // closed too
						   // so you should copy the contents to some other object.
						   // the result set is invalidated also if you recycle a Statement
						   // and try to execute some other query before the result set has been
						   // completely examined.
			
			return values;
		}
		
		/**
		 * Returns the value specified in the index column
		 * 
		 * @param rs			contains the result of the query
		 * @param columnIndex	column from where the value is retrieved
		*/
		private List<String> getValues( final ResultSet rs, final int columnIndex ) throws SQLException
		{
			// the order of the rows in a cursor
			// are implementation dependent unless you use the SQL ORDER statement
			ResultSetMetaData meta = rs.getMetaData();
			int colmax = meta.getColumnCount();
			List<String> values = new ArrayList<>();
			
			// the result set is a cursor into the data.  You can only
			// point to one row at a time
			// assume we are pointing to BEFORE the first row
			// rs.next() points to next row and returns true
			// or false if there is no next row, which breaks the loop
			while(rs.next()) {
				// if there are multiple versions, save the last one
				for(int i = 0; i < colmax; i++) {
					// In SQL the first column is indexed with 1 not 0
					if(i < values.size())
						values.set( i, rs.getString( i + 1 ) );
					else
						values.add( rs.getString( i + 1 ) );
				}
			}
			
			return values;
		}
		
		public void shutdown()
		{
			try {
				Statement st = conn.createStatement();
				
				// db writes out to files and performs clean shuts down
				// otherwise there will be an unclean shutdown
				// when program ends
				st.execute( "SHUTDOWN" );
				conn.close();    // if there are no other open connections
			}
		    catch( SQLException e ){}
		}
	}
}