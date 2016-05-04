/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import distributed_fs.net.messages.Message;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.VectorClockInconsistencyResolver;
import distributed_fs.versioning.Versioned;

public class DFSDatabase
{
	private final FileManagerThread _fileMgr;
	private final ScanDBThread scanDBThread;
	private CheckHintedHandoffDatabase hhThread;
	private final VersioningDatabase vDatabase;
	private boolean shutDown = false;
	private NavigableMap<ByteBuffer, DistributedFile> database;
	
	public static final Logger LOGGER = Logger.getLogger( DFSDatabase.class );
	
	/**
	 * Construct a new Distributed File System database.
	 * 
	 * @param fileMgr	the manager used to send/receive files.
	 * 					If {@code null} the database for the hinted handoff
	 * 					nodes does not start.
	*/
	public DFSDatabase( final FileManagerThread fileMgr ) throws IOException
	{
		_fileMgr = fileMgr;
		if(_fileMgr != null) {
			hhThread = new CheckHintedHandoffDatabase();
			hhThread.start();
		}
		
		try{ vDatabase = new VersioningDatabase( "Database/DFSdatabase" ); }
		catch( Exception e ) {
			e.printStackTrace();
			throw new IOException();
		}
		
		database = new TreeMap<>();
		loadFiles( new File( Utils.RESOURCE_LOCATION ), _fileMgr != null );
		
		scanDBThread = new ScanDBThread( this );
		scanDBThread.start();
	}
	
	/**
	 * Load recursively the files present in the current folder.
	 * 
	 * @param dir				the current directory
	 * @param makeSignature		
	*/
	private void loadFiles( final File dir, final boolean makeSignature ) throws IOException
	{
		for(File f : dir.listFiles()) {
			String fileName = f.getPath();
			if(f.isDirectory() && !fileName.endsWith( "/" ))
				fileName += "/";
			
			ByteBuffer fileId = Utils.getId( fileName );
			LOGGER.debug( "File: " + fileName + ", Directory: " + f.isDirectory() + ", Id: " + Utils.bytesToHex( fileId.array() ) );
			
			VectorClock clock = null;
			boolean deleted = false;
			String hintedHandoff = null;
			
			try{
				// get the version from the versioning database
				String statement = "SELECT vClock, hintedHandoff, Deleted FROM versions WHERE fileId = '" + Utils.bytesToHex( fileId.array() ) + "'";
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
				// save the version on its database, since it's not present
				try { saveVersion( Utils.bytesToHex( fileId.array() ), Utils.serializeObject( clock ), "", deleted, 0, false ); }
				catch( SQLException e ) { e.printStackTrace(); }
			}
			
			DistributedFile file = new DistributedFile( fileName, clock, makeSignature );
			file.setDeleted( deleted );
			if(hintedHandoff != null && hhThread != null)
				hhThread.saveFile( hintedHandoff, file );
			
			database.put( fileId, file );
			
			if(f.isDirectory())
				loadFiles( f, makeSignature );
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
	 * @param file			name of the file to save
	 * @param hintedHandoff	the hinted handoff address in the form {@code ipAddress:port}
	 * @param saveOnDisk	{@code true} if the file has to be saved on disk,
	 * 						{@code false} otherwise
	 * 
	 * @return {@code true} if the file has been added to the database,
	 * 		   {@code false} otherwise
	*/
	public boolean saveFile( final RemoteFile file, final String hintedHandoff, final boolean saveOnDisk ) throws IOException, SQLException
	{
		return saveFile( file.getName(), file.getContent(),
						 file.getVersion(), hintedHandoff,
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
	 * @return {@code true} if the file has been added to the database,
	 * 		   {@code false} otherwise
	*/
	public synchronized boolean saveFile( final String fileName, final byte[] content,
										  final VectorClock clock, final String hintedHandoff,
										  final boolean saveOnDisk ) throws IOException, SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.get( fileId );
		
		System.out.println( "SAVE FILE: " + file );
		
		boolean updated = false;
		
		if(file != null) {
			// Resolve the (possible) inconsistency through the versions.
			if(!resolveVersions( file.getVersion(), clock )) {
				// The input version is newer than mine, then
				// it will override the current one.
				file.setVersion( clock );
				if(file.isDeleted())
					file.setDeleted( false );
				
				updated = true;
				
				if(saveOnDisk) {
					saveVersion( Utils.bytesToHex( fileId.array() ),
								 Utils.serializeObject( file.getVersion() ),
								 hintedHandoff, false, 0, true );
					Utils.saveFileOnDisk( fileName, content );
				}
				
				database.put( fileId, file );
				if(hintedHandoff != null)
					hhThread.saveFile( hintedHandoff, file );
			}
		}
		else {
			updated = true;
			
			if(saveOnDisk) {
				saveVersion( Utils.bytesToHex( fileId.array() ),
							 Utils.serializeObject( clock ),
							 hintedHandoff, false, 0, false );
				Utils.saveFileOnDisk( fileName, content );
			}
			
			file = new DistributedFile( fileName, clock, _fileMgr != null );
			database.put( fileId, file );
			if(hintedHandoff != null)
				hhThread.saveFile( hintedHandoff, file );
		}
		
		return updated;
	}
	
	/** 
	 * Removes a file on database and on disk.
	 * 
	 * @param fileName	name of the file to remove
	 * @param clock		actual version of the file
	 * 
	 * @return {@code true} if the file has been removed from the database,
	 * 		   {@code false} otherwise
	*/
	public synchronized boolean removeFile( final String fileName, final VectorClock clock ) throws SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.get( fileId );
		boolean updated = false;
		
		// check whether the input version is newer than mine
		if(file == null || !resolveVersions( file.getVersion(), clock )) {
			if(file == null)
				file = new DistributedFile( fileName, clock );
			updated = true;
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
		
		Utils.deleteFileOnDisk( file.getName() );
	}

	/**
	 * Resolve the (possible) inconsistency through the versions.
	 * 
	 * @param versions	list of versions
	 * 
	 * @return the value specified by the {@code T} type.
	*/
	private boolean resolveVersions( final VectorClock myClock, final VectorClock vClock )
	{
		List<Versioned<Integer>> versions = new ArrayList<>();
		versions.add( new Versioned<Integer>( 0, myClock ) );
		versions.add( new Versioned<Integer>( 1, vClock ) );
		
		// get the list of concurrent versions
		VectorClockInconsistencyResolver<Integer> vecResolver = new VectorClockInconsistencyResolver<>();
		List<Versioned<Integer>> inconsistency = vecResolver.resolveConflicts( versions );
		
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
		database.clear();
	}
	
	/**
	 * Class used to periodically test
	 * if a file has to be removed from the database,
	 * using the LRU mechanism.
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
				catch( InterruptedException e ){}
				
				List<DistributedFile> files = database.getAllFiles();
				for(int i = files.size() - 1; i >= 0; i--) {
					DistributedFile file = files.get( i );
					if(file.isDeleted()) {
						if(file.checkDelete()) {
							try { database.removeFile( file.getName(), file.getVersion() ); }
							catch( SQLException e ) {}
						}
						else {
							// update the Time To Live of the file
							try{
								saveVersion( Utils.bytesToHex( file.getId() ),
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
		private final NavigableMap<String, List<DistributedFile>> hhDatabase;
		
		private static final int CHECK_TIMER = 5000;
		
		public CheckHintedHandoffDatabase()
		{
			upNodes = new LinkedList<String>();
			hhDatabase = new ConcurrentSkipListMap<String, List<DistributedFile>>();
		}
		
		@Override
		public void run()
		{
			while(!shutDown) {
				try{ Thread.sleep( CHECK_TIMER ); }
				catch( InterruptedException e ){}
				
				//LOGGER.debug( "HH_DATABASE: " + hhDatabase.size() + ", FILES: " + hhDatabase );
				
				for(String address : upNodes) {
					List<DistributedFile> files = hhDatabase.get( address );
					for(int i = files.size() - 1; i >= 0; i --) {
						if(files.get( i ).checkDelete()) {
							try { removeFile( files.get( i ).getName(), files.get( i ).getVersion() ); }
							catch ( SQLException e ) {}
							files.remove( i );
						}
					}
					
					// retrieve the informations from the saved address
					String[] data = address.split( ":" );
					String host = data[0];
					int port = Integer.parseInt( data[1] ) + 1;
					
					if(_fileMgr.sendFiles( port, Message.PUT, files, host, null, true, null, false ) ) {
						for(DistributedFile file : files) {
							String name = file.getName();
							try { removeFile( name, file.getVersion() ); }
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
	}
}