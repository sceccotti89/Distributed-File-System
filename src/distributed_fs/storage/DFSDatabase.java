/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.map.LinkedMap;
import org.apache.log4j.Logger;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.google.common.base.Preconditions;

import distributed_fs.client.DFSService.DBListener;
import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.VersioningUtils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Versioned;
import gossiping.event.GossipState;

public class DFSDatabase implements Closeable
{
	private final FileTransferThread _fileMgr;
	private final ScanDBThread scanDBThread;
	private CheckHintedHandoffDatabase hhThread;
	private final AsyncDiskWriter asyncWriter;
	private boolean shutDown = false;
	private DBListener listener;
	
	private String root;
	private DB db;
	private BTreeMap<String, DistributedFile> database;
	
	private boolean disableAsyncWrites = false;

	private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock( true );
	private static final ReadLock LOCK_READERS = LOCK.readLock();
	private static final WriteLock LOCK_WRITERS = LOCK.writeLock();
	
	/* Resources path locations. */
	private static final String RESOURCES_LOCATION = "Resources/";
	private static final String DATABASE_LOCATION = "Database/";
	
	private static final Logger LOGGER = Logger.getLogger( DFSDatabase.class );
	
	/**
	 * Construct a new Distributed File System database.
	 * 
	 * @param resourcesLocation		location of the files on disk.
	 *                              If {@code null}, will be set
	 * 								the default one ({@link DFSUtils#RESOURCE_LOCATION}).
	 * @param databaseLocation		location of the database resources.
	 *                              If {@code null}, will be set
	 * 								the default one ({@link VersioningDatabase#DB_LOCATION}).
	 * @param fileMgr				the manager used to send/receive files.
	 *								If {@code null} the database for the hinted handoff
	 * 								nodes does not start
	*/
	public DFSDatabase( final String resourcesLocation,
						final String databaseLocation,
						final FileTransferThread fileMgr ) throws IOException, DFSException
	{
		_fileMgr = fileMgr;
		if(_fileMgr != null) {
			hhThread = new CheckHintedHandoffDatabase();
			hhThread.start();
		}
		
		root = createResourcePath( resourcesLocation, RESOURCES_LOCATION );
		if(!DFSUtils.createDirectory( root )) {
			throw new DFSException( "Invalid resources path " + root + ".\n" +
									"Make sure that the path is correct and that " +
			                        "you have the permissions to create and execute it." );
		}
		LOGGER.info( "Resources created on: " + root );
		
		String dbRoot = createResourcePath( databaseLocation, DATABASE_LOCATION );
        if(!DFSUtils.createDirectory( dbRoot )) {
            throw new DFSException( "Invalid database path " + dbRoot + ".\n" +
                                    "Make sure that the path is correct and that " +
                                    "you have the permissions to create and execute it." );
        }
        LOGGER.info( "Database created on: " + dbRoot );
		
        //db = DBMaker.newFileDB( new File( dbRoot + "database.db" ) ).make();
        //database = db.getTreeMap( "map" );
        db = DBMaker.fileDB( new File( dbRoot + "database.db" ) )
                    .snapshotEnable()
                    .make();
        database = db.treeMap( "map" );
        //db.commit();
        
        //System.out.println( "FILES1: " + database.values() );
        
        // Check if the files in the database are present on disk.
        for(DistributedFile file : database.values()) {
            // If the file is no more on disk it will be deleted.
            if(!DFSUtils.existFile( root + file.getName(), false ))
                database.remove( file.getId() );
        }
        loadFiles( new File( root ) );
		
        db.commit();
		//System.out.println( "FILES2: " + database.values() );
		
		scanDBThread = new ScanDBThread();
		scanDBThread.start();
		
		asyncWriter = new AsyncDiskWriter();
		asyncWriter.start();
	}
	
	/**
	 * Returns the "normalized" path to the resources.
	 * 
	 * @param path			the path to the resources
	 * @param defaultPath	the default path if {@code path} is {@code null}
	*/
	private String createResourcePath( final String path, final String defaultPath )
	{
		String root = (path != null) ? path.replace( "\\", "/" ) : defaultPath;
		if(root.startsWith( "./" ))
			root = root.substring( 2 );
		
		root = new File( root ).getAbsolutePath().replace( "\\", "/" );
		if(!root.endsWith( "/" ))
			root += "/";
        
        return root;
	}
	
	/**
	 * Loads recursively the files present in the current folder.
	 * 
	 * @param dir	the current directory
	*/
	private void loadFiles( final File dir ) throws IOException
	{
		for(File f : dir.listFiles()) {
			String fileName = f.getPath().replace( "\\", "/" );
			if(f.isDirectory()) {
				loadFiles( f );
				fileName += "/";
			}
			fileName = normalizeFileName( fileName );
			
			DistributedFile file = database.get( DFSUtils.getId( fileName ) );
			if(file == null) {
			    file = new DistributedFile( fileName, f.isDirectory(), new VectorClock(), null );
			    database.put( file.getId(), file );
			}
			else {
				if(file.getHintedHandoff() != null && hhThread != null)
					hhThread.saveFile( file );
			}
			
			LOGGER.debug( "Loaded file: " + file );
		}
	}
	
	/**
     * By default all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     * <p/>
     * It is possible to disable Background Writer Thread, but this greatly hurts concurrency.
     * Without async writes, all threads blocks until all previous writes are not finished (single big lock).
     *
     * <p/>
     * This may workaround some problems.
     *
     * @return this builder
    */
	public DFSDatabase disableAsyncWrites() {
	    disableAsyncWrites = true;
	    return this;
	}
	
	/**
	 * Adds a new database listener.
	*/
	public void addListener( final DBListener listener ) {
	    this.listener = listener;
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
						 clock, hintedHandoff, saveOnDisk );
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
	public VectorClock saveFile( String fileName, final byte[] content,
								 final VectorClock clock, final String hintedHandoff,
								 final boolean saveOnDisk ) throws IOException, SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		VectorClock updated = null;
		fileName = normalizeFileName( fileName );
		String fileId = DFSUtils.getId( fileName );
		
		//System.out.println( "IN ATTESA DEL LOCK " + LOCK_WRITERS.getHoldCount() );
		LOCK_WRITERS.lock();
		//System.out.println( "LOCK ACQUISITO: " + !db.isClosed() );
		if(db.isClosed()) {
		    LOCK_WRITERS.unlock();
		    return null;
		}
		
		DistributedFile file = database.get( fileId );
		
		if(file != null) {
			//System.out.println( "OLD CLOCK: " + file.getVersion() );
			//System.out.println( "NEW CLOCK: " + clock );
			
			if(!resolveVersions( file.getVersion(), clock )) {
				// The input version is newer than mine, then
				// it overrides the current one.
				file.setVersion( updated = clock.clone() );
				if(file.isDeleted())
					file.setDeleted( false );
				file.setHintedHandoff( hintedHandoff );
				doSave( file, content, saveOnDisk );
			}
		}
		else {
			//file = new DistributedFile( fileName, root, clock, _fileMgr != null );
			file = new DistributedFile( fileName, new File( root + fileName ).isDirectory(), updated = clock.clone(), hintedHandoff );
			doSave( file, content, saveOnDisk );
		}
		
		if(hintedHandoff != null && hhThread != null)
            hhThread.saveFile( file );
		
		//System.out.println( "UPDATED: " + updated );
    	
		LOCK_WRITERS.unlock();
		
		if(updated != null && listener != null)
            listener.dbEvent( fileName, Message.GET );
		
		return updated;
	}
	
	private void doSave( final DistributedFile file,
						 final byte[] content,
						 final boolean saveOnDisk ) throws SQLException, IOException
	{
		database.put( file.getId(), file );
		db.commit();
		
		if(saveOnDisk) {
			if(disableAsyncWrites)
			    DFSUtils.saveFileOnDisk( root + file.getName(), content );
			else
			    asyncWriter.enqueue( content, root + file.getName(), Message.PUT );
		}
	}
	
	/**
	 * Removes a file on database and on disk.<br>
	 * The file is not intended to be definitely removed from the database,
	 * but marked as deleted.
	 * The file is keeped on database until its TimeToLive is not expired.
	 * 
	 * @param fileName        name of the file to remove
	 * @param clock           actual version of the file
	 * @param hintedHandoff   the hinted handoff address
	 * 
	 * @return the new clock, if updated, {@code null} otherwise.
	*/
	public VectorClock removeFile( String fileName,
	                               final VectorClock clock,
	                               final String hintedHandoff ) throws SQLException
	{
		Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		Preconditions.checkNotNull( clock,    "clock cannot be null." );
		
		VectorClock updated = null;
		fileName = normalizeFileName( fileName );
		String fileId = DFSUtils.getId( fileName );
		
		LOCK_WRITERS.lock();
		if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return null;
        }
        
		DistributedFile file = database.get( fileId );
		
		// Check whether the input version is newer than mine.
		if(file == null || !resolveVersions( file.getVersion(), clock )) {
		    updated = clock.clone();
            if(file == null)
                file = new DistributedFile( fileName, new File( root + fileName ).isDirectory(), clock, hintedHandoff );
            else
                file.setVersion( clock );
            
            file.setDeleted( true );
            
            database.put( fileId, file );
            db.commit();
            
            if(disableAsyncWrites)
                DFSUtils.deleteFileOnDisk( root + fileName );
            else
                asyncWriter.enqueue( null, root + fileName, Message.DELETE );
        }
    	
		LOCK_WRITERS.unlock();
		
		if(updated != null && listener != null)
            listener.dbEvent( fileName, Message.DELETE );
		
		return updated;
	}
	
	/**
	 * Resolve the (possible) inconsistency through the versions.
	 * 
	 * @param myClock	the current vector clock
	 * @param vClock	the input vector clock
	 * 
	 * @return {@code true} if the own file is the newest one,
	 * 		   {@code false} otherwise.
	*/
	private boolean resolveVersions( final VectorClock myClock, final VectorClock vClock )
	{
		List<Versioned<Integer>> versions = new ArrayList<>();
		versions.add( new Versioned<Integer>( 0, myClock ) );
		versions.add( new Versioned<Integer>( 1, vClock ) );
		
		// Get the list of concurrent versions.
		//VectorClockInconsistencyResolver<Integer> vecResolver = new VectorClockInconsistencyResolver<>();
		//List<Versioned<Integer>> inconsistency = vecResolver.resolveConflicts( versions );
		List<Versioned<Integer>> inconsistency = VersioningUtils.resolveVersions( versions );
		
		if(inconsistency.size() == 1)
			return inconsistency.get( 0 ).getValue() == 0;
		
		// Resolve the conflicts, using a time-based resolver.
		TimeBasedInconsistencyResolver<Integer> resolver = new TimeBasedInconsistencyResolver<>();
		int id = resolver.resolveConflicts( inconsistency ).get( 0 ).getValue();
		
		return id == 0;
	}
	
	/**
	 * Removes definitely a file from the database.
	 * 
	 * @param file  the file to delete
	*/
	private void deleteFile( final DistributedFile file ) throws SQLException
	{
	    LOCK_WRITERS.lock();
	    if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return;
        }
	    
	    if(file.isDeleted()) {
		    database.remove( file.getId() );
		    db.commit();
		    hhThread.removeFile( file );
	    }
        
        LOCK_WRITERS.unlock();
	}
	
    /** 
	 * Checks if a key is contained in the database.
	 * 
	 * @param fileName	name of the file
	 * 
	 * @return TRUE if the file is contained, FALSE otherwise
	*/
	public boolean containsKey( final String fileName )
	{
	    boolean contains;
	    String fileId = DFSUtils.getId( fileName );
	    
	    LOCK_READERS.lock();
	    if(db.isClosed()) {
            LOCK_READERS.unlock();
            return false;
        }
		contains = database.containsKey( fileId );
		LOCK_READERS.unlock();
		
		return contains;
	}
	
	/** 
	 * Returns the keys in the range specified by the identifiers.
	 * 
	 * @param fromId	source node identifier
	 * @param destId	destination node identifier
	 * 
	 * @return The list of keys. It can be null if, at least, one of the input ids is null.
	*/
	public List<DistributedFile> getKeysInRange( final String fromId, final String destId )
	{
		if(fromId == null || destId == null)
			return null;
		
		List<DistributedFile> result = new ArrayList<>( 16 );
		
		LOCK_READERS.lock();
		if(db.isClosed()) {
            LOCK_READERS.unlock();
            return null;
        }
    	
        if(destId.compareTo( fromId ) >= 0)
			result.addAll( database.subMap( fromId, false, destId, true ).values() );
		else {
			result.addAll( database.tailMap( fromId, false ).values() );
			result.addAll( database.headMap( destId, true ).values() );
		}
        
        LOCK_READERS.unlock();
		
		return result;
	}
	
	/**
	 * Returns the file specified by the given file name.
	 * 
	 * @param fileName	name of the file
	*/
	public DistributedFile getFile( final String fileName )
	{
	    String fileId = DFSUtils.getId( normalizeFileName( fileName ) );
	    LOCK_READERS.lock();
	    if(db.isClosed()) {
            LOCK_READERS.unlock();
            return null;
        }
	    DistributedFile file = database.get( fileId );
        LOCK_READERS.unlock();
	    
	    return file;
	}
	
	/**
	 * Returns all the stored files.
	 * 
	 * @return list containing all the files stored in the database
	*/
	public List<DistributedFile> getAllFiles()
	{
	    LOCK_READERS.lock();
	    if(db.isClosed()) {
            LOCK_READERS.unlock();
            return null;
        }
	    List<DistributedFile>  files = new ArrayList<>( database.values() );
        LOCK_READERS.unlock();
	    
	    return files;
	}
	
	/**
	 * Checks the existence of a file in the application file system,
	 * starting from the root.
	 * 
	 * @param filePath	the file to search
	 * 
	 * @return {@code true} if the file is present,
	 * 		   {@code false} otherwise
	*/
	public boolean checkExistFile( final String filePath ) throws IOException
	{
	    String rootPath = root + normalizeFileName( filePath );
		return checkExistsInFileSystem( new File( root ), rootPath );
	}
	
	private boolean checkExistsInFileSystem( final File filePath, final String fileName )
	{
		File[] files = filePath.listFiles();
		if(files != null) {
			for(File file : files) {
				if(file.getPath().replace( "\\", "/" ).equals( fileName ))
					return true;
				
				if(file.isDirectory()) {
					if(checkExistsInFileSystem( file, fileName ))
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
	 * @param state         state of the node
	*/
	public void checkHintedHandoffMember( final String nodeAddress, final GossipState state )
	{
		hhThread.checkMember( nodeAddress, state );
	}
	
	/**
     * Checks if the file has the correct format.<br>
     * The normalization consists in removing all the
     * parts of the name that don't permit it to be independent
     * of the database location.<br>
     * The returned string is a generic version of it.
     * 
     * @param fileName  the input file
     * 
     * @return the normalized file name
    */
    public String normalizeFileName( String fileName )
    {
        fileName = fileName.replace( "\\", "/" ); // "Normalization" phase.
        String backup = fileName;
        if(fileName.startsWith( "./" ))
            fileName = fileName.substring( 2 );
        
        File f = new File( fileName );
        fileName = f.getAbsolutePath().replace( "\\", "/" ); // "Normalization" phase.
        if(f.isDirectory() && !fileName.endsWith( "/" ))
            fileName += "/";
        
        if(fileName.startsWith( root ))
            fileName = fileName.substring( root.length() );
        else
            fileName = backup;
        
        return fileName;
    }

    @Override
	public void close()
	{
		shutDown = true;
		
		if(hhThread != null) hhThread.shutDown();
		scanDBThread.shutDown();
		asyncWriter.shutDown();
		
		try { scanDBThread.join(); }
		catch( InterruptedException e ) {}
		try { asyncWriter.join(); }
        catch( InterruptedException e ) {}
		if(hhThread != null) {
			try { hhThread.join(); }
			catch( InterruptedException e ) {}
		}
		
		LOCK_WRITERS.lock();
		if(!db.isClosed())
		    db.close();
		LOCK_WRITERS.unlock();
	}
	
	/**
	 * Class used to periodically test
	 * if a file has to be removed from the database.
	*/
	private class ScanDBThread extends Thread
	{
		// Time to wait before to check the database (1 minute).
		private static final int CHECK_TIMER = 60000;
		
		public ScanDBThread() {}
		
		@Override
		public void run()
		{
		    List<DistributedFile> files;
		    
			while(!shutDown) {
				try{ Thread.sleep( CHECK_TIMER );}
				catch( InterruptedException e ){ break; }
				
				files = getAllFiles();
				for(int i = files.size() - 1; i >= 0; i--) {
					DistributedFile file = files.get( i );
					if(file.isDeleted()) {
						if(file.checkDelete()) {
						    try { deleteFile( file ); }
						    catch( SQLException e ) { e.printStackTrace(); }
						}
					}
				}
			}
		}
		
		public void shutDown()
		{
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
		/** Database containing the hinted handoff files; it manages objects of (dest. IPaddress:port, [list of files]) */
		private final Map<String, List<DistributedFile>> hhDatabase;
		
		private final ReentrantLock MUTEX_LOCK = new ReentrantLock( true );
		private static final int CHECK_TIMER = 5000;
		
		public CheckHintedHandoffDatabase()
		{
			upNodes = new LinkedList<String>();
			hhDatabase = new HashMap<String, List<DistributedFile>>();
		}
		
		@Override
		public void run()
		{
		    List<String> nodes;
		    
			while(!shutDown) {
				try{ Thread.sleep( CHECK_TIMER ); }
				catch( InterruptedException e ){
					break;
				}
				
				//LOGGER.debug( "HH_DATABASE: " + hhDatabase.size() + ", FILES: " + hhDatabase );
				
				MUTEX_LOCK.lock();
				nodes = new LinkedList<>( upNodes );
				MUTEX_LOCK.unlock();
				
				for(String address : nodes) {
					List<DistributedFile> files = getFiles( address );
					ListIterator<DistributedFile> it = files.listIterator();
					for(int i = files.size() - 1; i >= 0; i--) {
					    DistributedFile file = it.next();
						if(file.checkDelete()) {
						    try { deleteFile( file ); }
                            catch( SQLException e ) {}
							it.remove();
						}
					}
					
					// Retrieve the informations from the saved address.
					String[] data = address.split( ":" );
					String host = data[0];
					int port = Integer.parseInt( data[1] ) + 1;
					
					if(_fileMgr.sendFiles( host, port, files, true, null, null ) ) {
					    // If all the files have been successfully delivered,
					    // they are removed for the current database.
						for(DistributedFile file : files) {
						    try { deleteFile( file ); }
							catch ( SQLException e ) {}
						}
						
						removeAddress( address );
					}
				}
				
				//MUTEX_LOCK.unlock();
			}
		}
		
		/**
		 * Returns all the files associated with the address.
		 * 
		 * @param address     the input address
		*/
		private List<DistributedFile> getFiles( final String address )
		{
		    List<DistributedFile> files;
		    
		    MUTEX_LOCK.lock();
		    files = new ArrayList<>( hhDatabase.get( address ) );
		    MUTEX_LOCK.unlock();
		    
		    return files;
		}
		
		/**
		 * Saves a file for a hinted handoff replica node.
		 * 
		 * @param hintedHandoff		the node to which the file has to be sent, in the form {@code ipAddress:port}
		 * @param file				the corresponding file
		*/
		public void saveFile( final DistributedFile file )
		{
			//file.setHintedHandoff( hintedHandoff );
			
			MUTEX_LOCK.lock();
			
			List<DistributedFile> files = hhDatabase.get( file.getHintedHandoff() );
			if(files == null) files = new ArrayList<>( 8 );
			files.add( file );
			hhDatabase.put( file.getHintedHandoff(), files );
			
			MUTEX_LOCK.unlock();
		}
		
		/**
		 * Removes a file from the database.
		 * 
		 * @param file    the file to remove
		*/
		public void removeFile( final DistributedFile file )
		{
		    String address = file.getHintedHandoff();
		    if(address != null) {
                MUTEX_LOCK.lock();
                hhDatabase.get( address ).remove( file );
                MUTEX_LOCK.unlock();
            }
		}
		
		/**
		 * Removes an address from the database.
		 * 
		 * @param address   the hinted handoff address
		*/
		public void removeAddress( final String address )
		{
		    if(address != null) {
    		    MUTEX_LOCK.lock();
    		    if(hhDatabase.get( address ).isEmpty())
    		        hhDatabase.remove( address );
    		    MUTEX_LOCK.unlock();
		    }
		}
		
		/**
		 * Checks if a member is a hinted handoff replica node.
		 * 
		 * @param nodeAddress	address of the node
		 * @param state         state of the node
		*/
		public void checkMember( final String nodeAddress, final GossipState state )
		{
		    MUTEX_LOCK.lock();
		    
		    if(state == GossipState.DOWN)
		        upNodes.remove( nodeAddress );
		    else {
    			if(hhDatabase.containsKey( nodeAddress ))
    				upNodes.add( nodeAddress );
		    }
		    
		    MUTEX_LOCK.unlock();
		}
		
		public void shutDown()
		{
			interrupt();
		}
	}
	
	/**
	 * Class used to write asynchronously a file on disk.<br>
	 * It's implemented as a queue, storing files
	 * in a FIFO discipline.<br>
	 * It can be disabled but the database operations would be slowed,
	 * in particular for long files.
	*/
	private class AsyncDiskWriter extends Thread
	{
	    private final LinkedMap<String,QueueNode> files;
	    private final Lock lock = new ReentrantLock();
	    private final Condition notEmpty = lock.newCondition();
	    
	    public AsyncDiskWriter()
	    {
	        files = new LinkedMap<>( 64 );
        }
	    
	    @Override
	    public void run()
	    {
	        while(!shutDown) {
	            try {
	                QueueNode node = dequeue();
	                // Write or delete a file on disk.
	                if(node.opType == Message.PUT)
	                    DFSUtils.saveFileOnDisk( node.path, node.file );
	                else
	                    DFSUtils.deleteFileOnDisk( node.path );
                } catch ( InterruptedException e ) {
                    break;
                }
                catch( IOException e1 ) {
                    e1.printStackTrace();
                }
	        }
	    }
	    
	    private QueueNode dequeue() throws InterruptedException
	    {
	        lock.lock();
	        
	        while(files.isEmpty())
	            notEmpty.await();
	        
	        // Get and remove the first element of the queue.
	        QueueNode node = files.remove( 0 );
	        
	        lock.unlock();
	        
	        return node;
	    }
	    
	    /**
	     * Inserts a file into the queue.
	     * 
	     * @param file     the file's content. It may be {@code null} in a delete operation
	     * @param path     the file's location. It may be a relative or absolute path to the file
	     * @param opType   the operation to perform ({@code PUT} or {@code DELETE})
	    */
	    public void enqueue( final byte[] file, final String path, final byte opType )
	    {
	        lock.lock();
	        
	        boolean empty = files.isEmpty();
            files.put( path, new QueueNode( file, path, opType ) );
            if(empty)
                notEmpty.signal();
            
            lock.unlock();
	    }
	    
	    private class QueueNode
	    {
	        public final byte[] file;
	        public final String path;
	        public final byte opType;
	        
	        public QueueNode( final byte[] file, final String path, final byte opType )
	        {
                this.file = file;
                this.path = path;
                this.opType = opType;
            }
	    }
	    
	    public void shutDown()
        {
            interrupt();
        }
	}
}