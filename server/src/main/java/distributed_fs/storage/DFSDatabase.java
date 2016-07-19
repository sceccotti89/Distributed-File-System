/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
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

import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.Occurred;
import distributed_fs.versioning.VectorClock;
import gossiping.event.GossipState;

public class DFSDatabase extends DBManager implements Closeable
{
	private final FileTransfer _fileMgr;
	private final ScanDBThread scanDBThread;
	private CheckHintedHandoffDatabase hhThread;
	private final AsyncDiskWriter asyncWriter;
	private final List<String> toUpdate;
	
	private DB db;
	private BTreeMap<String, DistributedFile> database;
	
	private final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock( true );
	private final ReadLock LOCK_READERS = LOCK.readLock();
	private final WriteLock LOCK_WRITERS = LOCK.writeLock();
	
	// Resources path locations.
	private static final String RESOURCES_LOCATION = "Resources/";
	private static final String DATABASE_LOCATION = "Database/";
	
	private static final Logger LOGGER = Logger.getLogger( DFSDatabase.class );
	
	
	
	
	/**
	 * Construct a new Distributed File System database.
	 * 
	 * @param resourcesLocation		location of the files on disk.
	 *                              If {@code null}, will be set
	 * 								the default one ({@link #RESOURCES_LOCATION}).
	 * @param databaseLocation		location of the database resources.
	 *                              If {@code null}, will be set
	 * 								the default one ({@link #DATABASE_LOCATION}).
	 * @param fileMgr				the manager used to send/receive files.
	 *								If {@code null} the database for the hinted handoff
	 * 								nodes does not start
	*/
	public DFSDatabase( final String resourcesLocation,
						final String databaseLocation,
						final FileTransfer fileMgr ) throws IOException, DFSException
	{
	    _fileMgr = fileMgr;
		if(_fileMgr != null) {
			hhThread = new CheckHintedHandoffDatabase( this );
			hhThread.start();
		}
		
		root = createResourcePath( resourcesLocation, RESOURCES_LOCATION );
		if(!createDirectory( root )) {
			throw new DFSException( "Invalid resources path " + root + ".\n" +
									"Make sure that the path is correct and that " +
			                        "you have the permissions to create and execute it." );
		}
		LOGGER.info( "Resources created on: " + root );
		
		String dbRoot = createResourcePath( databaseLocation, DATABASE_LOCATION );
        if(!createDirectory( dbRoot )) {
            throw new DFSException( "Invalid database path " + dbRoot + ".\n" +
                                    "Make sure that the path is correct and that " +
                                    "you have the permissions to create and execute it." );
        }
        LOGGER.info( "Database created on: " + dbRoot );
		
        db = DBMaker.fileDB( new File( dbRoot + "database.db" ) )
                    .snapshotEnable()
                    .make();
        database = db.treeMap( "map" );
        //db.commit();
        
        toUpdate = new Vector<>();
        loadFiles();
        
        // Add the files to the hinted handoff database.
        for(DistributedFile file : database.values()) {
            if(file.getHintedHandoff() != null && hhThread != null)
                hhThread.saveFile( file );
        }
		
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
	 * 
	 * @return the normalized path
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
     * Loads all the files present in the file system,
     * starting from the root.
    */
    public void loadFiles() throws IOException
    {
        checkRemovedFiles();
        doLoadFiles( new File( root ) );
        db.commit();
    }
	
	private void doLoadFiles( final File dir ) throws IOException
	{
		for(File f : dir.listFiles()) {
			String fileName = f.getPath().replace( "\\", "/" );
			if(f.isDirectory()) {
			    doLoadFiles( f );
				fileName += "/";
			}
			fileName = normalizeFileName( fileName );
			
			DistributedFile file = database.get( DFSUtils.getId( fileName ) );
			if(file == null || file.isDeleted()) {
			    if(file == null) {
			        file = new DistributedFile( fileName, f.isDirectory(), new VectorClock(), null );
			        database.put( file.getId(), file );
			        notifyListeners( fileName, Message.GET );
			    }
			    
			    toUpdate.add( file.getName() );
			    LOGGER.debug( "Loaded file: " + file );
			}
		}
	}
	
	/**
	 * Checks if the files stored on database
	 * are still present on disk.
	*/
	private void checkRemovedFiles() throws IOException
	{
	    List<DistributedFile> files = getAllFiles();
	    
	    for(DistributedFile file : files) {
            // If the file is no more on disk it's definitely removed.
            if(!existFile( root + file.getName(), false ) && !file.isDeleted()) {
                LOCK_WRITERS.lock();
                database.remove( file.getId() );
                LOCK_WRITERS.unlock();
                
                notifyListeners( file.getName(), Message.DELETE );
                if(hhThread != null)
                    hhThread.removeFiles( file );
            }
        }
	}
	
	/**
	 * Returns the list of files that have to be updated.
	*/
	public List<String> getUpdateList() {
	    return toUpdate;
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
	public VectorClock saveFile( final DistributedFile file, final VectorClock clock,
								 final String hintedHandoff, final boolean saveOnDisk )
								         throws IOException
	{
		return saveFile( file.getName(), file.getContent(), clock, file.isDirectory(), hintedHandoff, saveOnDisk );
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
								 final VectorClock clock, final boolean isDirectory,
								 final String hintedHandoff, final boolean saveOnDisk )
								         throws IOException
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
		
		if(file == null) {
		    file = new DistributedFile( fileName, isDirectory, updated = clock.clone(), hintedHandoff );
            doSave( file, content, saveOnDisk );
		}
		else {
			if(updateVersions( file.getVersion(), clock )) {
				// The input version is newer than mine, then
				// it overrides the current one.
				file.setVersion( updated = clock.clone() );
				file.setDeleted( false );
				file.setHintedHandoff( hintedHandoff );
				doSave( file, content, saveOnDisk );
			}
		}
		
		if(hintedHandoff != null && hhThread != null)
            hhThread.saveFile( file );
    	
		LOCK_WRITERS.unlock();
		
		if(saveOnDisk && updated != null)
		    notifyListeners( fileName, Message.GET );
		
		return updated;
	}
	
	private void doSave( final DistributedFile file,
						 final byte[] content, final boolean saveOnDisk ) throws IOException
	{
		if(saveOnDisk) {
    		if(disableAsyncWrites)
    		    saveFileOnDisk( root + file.getName(), content );
    		else
    		    asyncWriter.enqueue( content, root + file.getName(), Message.PUT );
		}
		
		addParentFile( new File( file.getName() ).getParentFile(), saveOnDisk );
		
		database.put( file.getId(), file );
		db.commit();
	}
	
	/**
     * Add the parent of a file on database (if it doesn't exist).
     * 
     * @param f    the current file
    */
    private void addParentFile( final File f, final boolean saveOnDisk ) throws IOException
    {
        if(f == null)
            return;
        addParentFile( f.getParentFile(), saveOnDisk );
        
        // Add the file on database. It's obviously a directory.
        String fileName = normalizeFileName( f.getPath() );
        if(!database.containsKey( DFSUtils.getId( fileName ) )) {
            DistributedFile file = new DistributedFile( fileName, true, new VectorClock(), null );
            database.put( file.getId(), file );
            if(saveOnDisk) {
                if(disableAsyncWrites)
                    saveFileOnDisk( root + fileName, null );
                else
                    asyncWriter.enqueue( null, root + fileName, Message.PUT );
            }
            
            notifyListeners( fileName, Message.GET );
        }
    }
    
    /**
     * Deletes a file on database and on disk.<br>
     * The file is not intended to be definitely removed from the database,
     * but only marked as deleted.
     * The file is keep on database until its TimeToLive is not expired.
     * 
     * @param file            the file to remove
     * @param hintedHandoff   the hinted handoff address
     * 
     * @return the new clock, if updated, {@code null} otherwise.
    */
    public VectorClock deleteFile( final DistributedFile file, final String hintedHandoff )
    {
        return deleteFile( file.getName(), file.getVersion(),
                           file.isDirectory(), hintedHandoff );
    }
	
	/**
	 * Deletes a file on database and on disk.<br>
	 * The file is not intended to be definitely removed from the database,
	 * but only marked as deleted.
	 * The file is keep on database until its TimeToLive is not expired.
	 * 
	 * @param fileName        name of the file to remove
	 * @param clock           actual version of the file
	 * @param hintedHandoff   the hinted handoff address
	 * 
	 * @return the new clock, if updated, {@code null} otherwise.
	*/
	public VectorClock deleteFile( String fileName,
	                               final VectorClock clock,
	                               final boolean isDirectory,
	                               final String hintedHandoff )
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
		if(file == null || updateVersions( file.getVersion(), clock )) {
		    updated = clock.clone();
            if(file == null)
                file = new DistributedFile( fileName, isDirectory, clock, hintedHandoff );
            
            if(!file.isDeleted()) {
                file.setVersion( clock );
                file.setDeleted( true );
                
                database.put( fileId, file );
                
                if(disableAsyncWrites)
                    deleteFileOnDisk( root + fileName );
                else
                    asyncWriter.enqueue( null, root + fileName, Message.DELETE );
                
                if(isDirectory)
                    removeDirectory( new File( root + fileName ) );
                db.commit();
            }
        }
    	
		LOCK_WRITERS.unlock();
		
		if(updated != null)
            notifyListeners( fileName, Message.DELETE );
		
		return updated;
	}
	
	/**
	 * Delete recursively all the content of a folder.
	 * 
	 * @param dir  current directory
	*/
    private void removeDirectory( final File dir )
    {
        File[] files = dir.listFiles();
        if(files != null) {
            for(File f : files) {
                String fileName = f.getPath().replace( "\\", "/" );
                if(f.isDirectory()) {
                    removeDirectory( f );
                    fileName += "/";
                }
                fileName = normalizeFileName( fileName );
                
                DistributedFile file = database.get( DFSUtils.getId( fileName ) );
                if(file != null && !file.isDeleted()) {
                    file.setDeleted( true );
                    database.put( file.getId(), file );
                    if(disableAsyncWrites)
                        deleteFileOnDisk( root + fileName );
                    else
                        asyncWriter.enqueue( null, root + fileName, Message.DELETE );
                    notifyListeners( fileName, Message.DELETE );
                }
            }
        }
    }
	
	/**
	 * Resolve the (possible) inconsistency through the versions.
	 * 
	 * @param myClock	the current vector clock
	 * @param vClock	the input vector clock
	 * 
	 * @return {@code true} if the received file has the most updated version,
	 * 		   {@code false} otherwise.
	*/
	private boolean updateVersions( final VectorClock myClock, final VectorClock vClock )
	{
	    if(disableReconciliation)
	        return false;
	    
	    return vClock.compare( myClock ) == Occurred.AFTER;
	}
	
	/**
	 * Removes definitely a file from the database.
	 * 
	 * @param file  the file to delete
	*/
	private void removeFile( final DistributedFile file )
	{
	    LOCK_WRITERS.lock();
	    if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return;
        }
	    
	    if(file.isDeleted()) {
	        LOGGER.debug( "Removing file: " + file.getName() );
		    database.remove( file.getId() );
		    db.commit();
		    if(hhThread != null)
		        hhThread.removeFiles( file );
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
		
		List<DistributedFile> result = new ArrayList<>( 32 );
		
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
	 * Updates the lastModified parameter of the given file.
	 * 
	 * @param fileName         name of the file to update
	 * @param lastModified     the last modified value
	*/
	/*public void updateLastModified( final String fileName, final long lastModified )
	{
	    String fileId = DFSUtils.getId( normalizeFileName( fileName ) );
	    LOCK_WRITERS.lock();
        if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return;
        }
        
        DistributedFile file = database.get( fileId );
        if(file != null) {
            file.setLastModified( lastModified );
            database.put( fileId, file );
            db.commit();
        }
        
        LOCK_WRITERS.unlock();
	}*/
	
	/**
	 * Checks the existence of a file in the application file system,
	 * starting from the root.
	 * 
	 * @param filePath	the file to search
	 * 
	 * @return {@code true} if the file is present,
	 * 		   {@code false} otherwise
	*/
	public boolean checkExistsInFileSystem( final String filePath )
	{
	    String rootPath = root + normalizeFileName( filePath );
		return checkExistsFile( new File( root ), rootPath );
	}
	
	private boolean checkExistsFile( final File filePath, final String fileName )
	{
		File[] files = filePath.listFiles();
		if(files == null)
		    files = filePath.listFiles();
		
		if(files != null) {
			for(File file : files) {
			    String _file = file.getPath().replace( '\\', '/' );
				if(file.isDirectory() && !_file.endsWith( "/" ))
				    _file = _file + "/";
			    
				if(_file.equals( fileName ))
					return true;
				
				if(file.isDirectory()) {
					if(checkExistsFile( file, fileName ))
						return true;
				}
			}
		}
		
		return false;
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

    @Override
	public void close()
	{
		shutDown.set( true );
		
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
		
		LOGGER.info( "Database closed." );
	}
	
	/**
	 * Class used to periodically test
	 * if a file has to be definitely removed
	 * from the database.
	*/
	private class ScanDBThread extends Thread
	{
		// Time to wait before to check the database (1 minute).
		private static final int CHECK_TIMER = 60000;
		
		public ScanDBThread()
		{
		    setName( "ScanDB" );
		}
		
		@Override
		public void run()
		{
		    LOGGER.info( "Database scanner thread launched." );
		    
		    List<DistributedFile> files;
		    
			while(!shutDown.get()) {
				try{ Thread.sleep( CHECK_TIMER );}
				catch( InterruptedException e ){ break; }
				
				files = getAllFiles();
				for(int i = files.size() - 1; i >= 0; i--) {
					DistributedFile file = files.get( i );
					if(file.isDeleted()) {
						if(file.checkDelete())
						    removeFile( file );
					}
				}
			}
			
			LOGGER.info( "Database scanner thread closed." );
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
		private final DBManager db;
		
		private final ReentrantLock MUTEX_LOCK = new ReentrantLock( true );
		private static final int CHECK_TIMER = 10; // 10 minutes.
		
		public CheckHintedHandoffDatabase( final DBManager db )
		{
		    setName( "HintedHandoff" );
		    
			upNodes = new LinkedList<String>();
			hhDatabase = new HashMap<String, List<DistributedFile>>();
			
			this.db = db;
		}
		
		@Override
		public void run()
		{
		    LOGGER.info( "HintedHandoff thread launched." );
		    
		    List<String> nodes;
		    
			while(!shutDown.get()) {
				MUTEX_LOCK.lock();
				nodes = new LinkedList<>( upNodes );
				MUTEX_LOCK.unlock();
				
				for(String address : nodes) {
				    if(shutDown.get())
				        break;
				    
					List<DistributedFile> files = getFiles( address );
					ListIterator<DistributedFile> it = files.listIterator();
					for(int i = files.size() - 1; i >= 0; i--) {
					    DistributedFile file = it.next();
						if(!file.checkDelete())
						    file.loadContent( db );
						else {
						    removeFile( file );
							it.remove();
						}
					}
					
					// Retrieve the informations from the saved address.
					String[] data = address.split( ":" );
					String host = data[0];
					int port = Integer.parseInt( data[1] );
					
					if(_fileMgr.sendFiles( host, port, files, true, null, null ) ) {
					    // If all the files have been successfully delivered,
					    // they are removed from the current database.
						for(DistributedFile file : files)
						    removeFile( file );
						
						removeAddress( address );
					}
				}
				
				try{ TimeUnit.MINUTES.sleep( CHECK_TIMER ); }
                catch( InterruptedException e ){}
			}
			
			LOGGER.info( "HintedHandoff thread closed." );
		}
		
		/**
		 * Returns all the files associated to the address.
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
		public void removeFiles( final DistributedFile file )
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
		private void removeAddress( final String address )
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
		    boolean wakeUp = false;
		    
		    MUTEX_LOCK.lock();
		    
		    if(state == GossipState.DOWN)
		        upNodes.remove( nodeAddress );
		    else {
    			if(hhDatabase.containsKey( nodeAddress )) {
    				upNodes.add( nodeAddress );
    				wakeUp = true;
    			}
		    }
		    
		    MUTEX_LOCK.unlock();
		    
		    // Interrupt the node (if not awake),
		    // forcing it to send immediately the files.
            if(wakeUp)
                interrupt();
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
	 * in particular for huge files.
	*/
	private class AsyncDiskWriter extends Thread
	{
	    private final LinkedMap<String,QueueNode> files;
	    private final Lock lock = new ReentrantLock();
	    private final Condition notEmpty = lock.newCondition();
	    
	    public AsyncDiskWriter()
	    {
	        setName( "AsyncWriter" );
	        
	        files = new LinkedMap<>( 64 );
        }
	    
	    @Override
	    public void run()
	    {
	        LOGGER.info( "AsyncWriter thread launched." );
	        
	        while(!shutDown.get()) {
	            try {
	                QueueNode node = dequeue();
	                // Write or delete a file on disk.
	                if(node.opType == Message.PUT)
	                    saveFileOnDisk( node.path, node.file );
	                else
	                    deleteFileOnDisk( node.path );
                } catch ( InterruptedException e ) {
                    break;
                }
                catch( IOException e1 ) {
                    e1.printStackTrace();
                }
	        }
	        
	        LOGGER.info( "AsyncWriter thread closed." );
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