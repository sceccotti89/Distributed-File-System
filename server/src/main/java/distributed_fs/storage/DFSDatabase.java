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

import org.apache.commons.collections4.map.LinkedMap;
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
    // List of threads.
    private final FileTransfer _fileMgr;
    private ScanDBThread scanDBThread;
    private CheckHintedHandoffDatabase hhThread;
    private BackupThread bThread;
    
    private final List<String> toUpdate = new Vector<>( 64 );
    
    private DB db;
    private BTreeMap<String, DistributedFile> database;
    
    // Database path location.
    private static final String DATABASE_LOCATION = "Database/";
    
    
    
    
    
    /**
     * Construct a new Distributed File System database.
     * 
     * @param resourcesLocation        location of the files on disk.
     *                              If {@code null}, will be set
     *                                 the default one ({@link #RESOURCES_LOCATION}).
     * @param databaseLocation        location of the database resources.
     *                              If {@code null}, will be set
     *                                 the default one ({@link #DATABASE_LOCATION}).
     * @param fileMgr                the manager used to send/receive files.
     *                                If {@code null} the database for the hinted handoff
     *                                 nodes does not start
    */
    public DFSDatabase( String resourcesLocation,
                        String databaseLocation,
                        FileTransfer fileMgr ) throws DFSException
    {
        super( resourcesLocation );
        
        _fileMgr = fileMgr;
        if(_fileMgr != null) {
            hhThread = new CheckHintedHandoffDatabase( this );
            hhThread.start();
        }
        
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
    }
    
    /**
     * Sets the backup location.
     * 
     * @param dbBackupPath     the database backup location
     * @param resBackupPath    the resources backup location
    */
    public void setBackup( String dbBackupPath, String resBackupPath ) throws DFSException, IOException
    {
        String dbRoot = createResourcePath( dbBackupPath, null );
        if(!createDirectory( dbRoot )) {
            throw new DFSException( "Invalid database path " + dbRoot + ".\n" +
                                    "Make sure that the path is correct and that " +
                                    "you have the permissions to create and execute it." );
        }
        LOGGER.info( "Backup database created on: " + dbRoot );
        
        bThread = new BackupThread( dbBackupPath, resBackupPath );
        bThread.start();
    }
    
    /**
     * Creates a new instance of the database.
    */
    public void newInstance() throws IOException
    {
        launched.set( true );
        
        loadFiles();
        
        // Save the files in the hinted handoff database.
        for(DistributedFile file : database.values()) {
            if(file.getHintedHandoff() != null && hhThread != null)
                hhThread.saveFile( file );
        }
        
        if(!disableScanDB) {
            scanDBThread = new ScanDBThread();
            scanDBThread.start();
        }
        
        if(!disableAsyncWrites)
            asyncWriter = new AsyncDiskWriter( this, true );
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
    
    private void doLoadFiles( File dir ) throws IOException
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
                    
                    if(bThread != null) {
                        file.loadContent( this );
                        bThread.enqueue( file, file.getContent(), BackupThread.PUT );
                    }
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
                notifyListeners( file.getName(), Message.DELETE );
                LOCK_WRITERS.unlock();
                
                if(hhThread != null)
                    hhThread.removeFiles( file );
                
                if(bThread != null)
                    bThread.enqueue( file, null, BackupThread.DELETE );
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
     * Saves a file on the database.
     * 
     * @param file                name of the file to save
     * @param clock                the associated vector clock
     * @param hintedHandoff        the hinted handoff address in the form {@code ipAddress:port}
     * @param saveOnDisk        {@code true} if the file has to be saved on disk,
     *                             {@code false} otherwise
     * 
     * @return the new clock, if updated, {@code null} otherwise
    */
    public VectorClock saveFile( DistributedFile file, VectorClock clock,
                                 String hintedHandoff, boolean saveOnDisk )
                                         throws IOException
    {
        return saveFile( file.getName(), file.getContent(), clock,
                         file.isDirectory(), hintedHandoff, saveOnDisk );
    }
    
    /**
     * Saves a file on the database.
     * 
     * @param filePath         path of the file to save
     * @param content          file's content
     * @param clock            the associated vector clock
     * @param isDirectory      {@code true} if the file is a directory, {@code false} otherwise
     * @param hintedHandoff    the hinted handoff address in the form {@code ipAddress:port}
     * @param saveOnDisk       {@code true} if the file has to be saved on disk,
     *                         {@code false} otherwise
     * 
     * @return the new clock, if updated, {@code null} otherwise.
    */
    public VectorClock saveFile( String filePath, byte[] content,
                                 VectorClock clock, boolean isDirectory,
                                 String hintedHandoff, boolean saveOnDisk )
                                         throws IOException
    {
        Preconditions.checkNotNull( filePath, "filePath cannot be null." );
        Preconditions.checkNotNull( clock,    "clock cannot be null." );
        
        VectorClock updated = null;
        String fileName = normalizeFileName( filePath );
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
            if(updateVersion( file.getVersion(), clock )) {
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
        
        if(saveOnDisk && updated != null)
            notifyListeners( fileName, Message.GET );
        
        LOCK_WRITERS.unlock();
        
        if(updated != null && bThread != null)
            bThread.enqueue( file, content, BackupThread.PUT );
        
        return updated;
    }
    
    private void doSave( DistributedFile file,
                         byte[] content,
                         boolean saveOnDisk ) throws IOException
    {
        if(saveOnDisk) {
            if(disableAsyncWrites) {
                writeFileOnDisk( root + file.getName(), content );
            } else {
                asyncWriter.enqueue( content, root + file.getName(), Message.PUT );
            }
        }
        
        addParentFile( new File( file.getName() ).getParentFile(), saveOnDisk );
        
        database.put( file.getId(), file );
        db.commit();
    }
    
    /**
     * Adds the parent of a file on database (if it doesn't exist).
     * 
     * @param f             the current file.
     * @param saveOnDisk    {@code true} if the file have to be saved on disk,
     *                      {@code false} otherwise.
    */
    private void addParentFile( File f, boolean saveOnDisk ) throws IOException
    {
        if(f == null) {
            return;
        }
        
        addParentFile( f.getParentFile(), saveOnDisk );
        
        // Add the file on database. It's obviously a directory.
        String fileName = normalizeFileName( f.getPath() );
        
        if(!database.containsKey( DFSUtils.getId( fileName ) )) {
            DistributedFile file = new DistributedFile( fileName, true, new VectorClock(), null );
            database.put( file.getId(), file );
            
            if(saveOnDisk) {
                if(disableAsyncWrites) {
                    writeFileOnDisk( root + fileName, null );
                } else {
                    asyncWriter.enqueue( null, root + fileName, Message.PUT );
                }
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
    public VectorClock deleteFile( DistributedFile file, String hintedHandoff ) {
        return deleteFile( file.getName(), file.getVersion(), file.isDirectory(), hintedHandoff );
    }
    
    /**
     * Deletes a file on database and on disk.<br>
     * The file is not intended to be definitely removed from the database,
     * but only marked as deleted.
     * The file is keep on database until its TimeToLive is not expired.
     * 
     * @param filePath        path to the file to remove
     * @param clock           actual version of the file
     * @param isDirectory     {@code true} if the file is a directory, {@code false} otherwise
     * @param hintedHandoff   the hinted handoff address
     * 
     * @return the new clock, if updated, {@code null} otherwise.
    */
    public VectorClock deleteFile( String filePath, VectorClock clock,
                                   boolean isDirectory, String hintedHandoff )
    {
        Preconditions.checkNotNull( filePath, "filePath cannot be null." );
        Preconditions.checkNotNull( clock,    "clock cannot be null." );
        
        VectorClock updated = null;
        String fileName = normalizeFileName( filePath );
        String fileId = DFSUtils.getId( fileName );
        
        LOCK_WRITERS.lock();
        if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return null;
        }
        
        DistributedFile file = database.get( fileId );
        
        // Check whether the input version is newer than mine.
        if(file == null || updateVersion( file.getVersion(), clock )) {
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
                    removeDirectory( new File( root + fileName ), clock.getLastNodeId() );
                db.commit();
            }
            
            notifyListeners( fileName, Message.DELETE );
        }
        
        LOCK_WRITERS.unlock();
        
        if(updated != null && bThread != null)
            bThread.enqueue( file, null, BackupThread.DELETE );
        
        return updated;
    }
    
    /**
     * Delete recursively all the content of a folder.
     * 
     * @param dir       current directory
     * @param nodeId    associated node identifier
    */
    private void removeDirectory( File dir, String nodeId )
    {
        File[] files = dir.listFiles();
        if(files != null) {
            for(File f : files) {
                String fileName = normalizeFileName( f.getPath().replace( "\\", "/" ) );
                if(f.isDirectory())
                    removeDirectory( f, nodeId );
                
                DistributedFile file = database.get( DFSUtils.getId( fileName ) );
                if(file != null && !file.isDeleted()) {
                    file.setDeleted( true );
                    file.incrementVersion( nodeId );
                    database.put( file.getId(), file );
                    
                    if(disableAsyncWrites) {
                        deleteFileOnDisk( root + fileName );
                    } else {
                        asyncWriter.enqueue( null, root + fileName, Message.DELETE );
                    }
                    notifyListeners( fileName, Message.DELETE );
                }
            }
        }
    }
    
    /**
     * Resolves the (possible) inconsistency through the versions.
     * 
     * @param myClock    the current vector clock
     * @param vClock     the input vector clock
     * 
     * @return {@code true} if the received file has the most updated version,
     *         {@code false} otherwise.
    */
    private boolean updateVersion( VectorClock myClock, VectorClock vClock )
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
    private void removeFile( DistributedFile file )
    {
        boolean deleted = false;
        
        LOCK_WRITERS.lock();
        
        if(db.isClosed()) {
            LOCK_WRITERS.unlock();
            return;
        }
        
        if(file.isDeleted()) {
            deleted = true;
            LOGGER.debug( "Removing file: " + file.getName() );
            database.remove( file.getId() );
            db.commit();
            
            if(hhThread != null)
                hhThread.removeFiles( file );
        }
        
        LOCK_WRITERS.unlock();
        
        if(deleted && bThread != null)
            bThread.enqueue( file, null, BackupThread.REMOVE );
    }
    
    /** 
     * Checks if a key is contained in the database.
     * 
     * @param fileName    name of the file
     * 
     * @return TRUE if the file is contained, FALSE otherwise
    */
    public boolean containsKey( String fileName )
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
     * @param fromId    source node identifier
     * @param destId    destination node identifier
     * 
     * @return The list of keys. It can be null if, at least, one of the input ids is null.
    */
    public List<DistributedFile> getKeysInRange( String fromId, String destId )
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
     * @param fileName    name of the file
    */
    public DistributedFile getFile( String fileName )
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
     * Checks if a member is a hinted handoff replica node.
     * 
     * @param nodeAddress    address of the node
     * @param state         state of the node
    */
    public void checkHintedHandoffMember( String nodeAddress, GossipState state )
    {
        hhThread.checkMember( nodeAddress, state );
    }

    @Override
    public void close()
    {
        shutDown.set( true );
        
        if(hhThread != null) hhThread.shutDown();
        if(scanDBThread != null) scanDBThread.shutDown();
        asyncWriter.shutDown();
        
        if(scanDBThread != null) {
            try { scanDBThread.join(); }
            catch( InterruptedException e ) {}
        }
        
        if(hhThread != null) {
            try { hhThread.join(); }
            catch( InterruptedException e ) {}
        }
        
        LOCK_WRITERS.lock();
        if(!db.isClosed())
            db.close();
        LOCK_WRITERS.unlock();
        
        if(bThread != null) {
            try { bThread.join(); }
            catch( InterruptedException e ) {}
        }
        
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
        
        public CheckHintedHandoffDatabase( DBManager db )
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
        private List<DistributedFile> getFiles( String address )
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
         * @param hintedHandoff        the node to which the file has to be sent, in the form {@code ipAddress:port}
         * @param file                the corresponding file
        */
        public void saveFile( DistributedFile file )
        {
            MUTEX_LOCK.lock();
            
            List<DistributedFile> files = hhDatabase.get( file.getHintedHandoff() );
            if(files == null) files = new ArrayList<>( 64 );
            files.add( file );
            hhDatabase.put( file.getHintedHandoff(), files );
            
            MUTEX_LOCK.unlock();
        }
        
        /**
         * Removes a file from the database.
         * 
         * @param file    the file to remove
        */
        public void removeFiles( DistributedFile file )
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
        private void removeAddress( String address )
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
         * @param nodeAddress    address of the node
         * @param state         state of the node
        */
        public void checkMember( String nodeAddress, GossipState state )
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
     * Thread used to perform the operations in background,
     * writing in a backup database.
    */
    private class BackupThread extends Thread
    {
        private DFSDatabase backup;
        private final LinkedMap<String, QueueNode> filesQueue;
        private final Lock lock;
        private final Condition notEmpty;
        
        private static final byte PUT = 0, DELETE = 1, REMOVE = 2;
        
        
        
        public BackupThread( String dbBackupPath, String resBackupPath ) throws IOException, DFSException
        {
            backup = new DFSDatabase( resBackupPath, dbBackupPath, null );
            backup.disableAsyncWrites();
            backup.disableScanDB();
            
            filesQueue = new LinkedMap<>( 64 );
            lock = new ReentrantLock();
            notEmpty = lock.newCondition();
        }
        
        @Override
        public void run()
        {
            QueueNode node;
            while(!shutDown.get()) {
                try {
                    node = dequeue();
                    if(node == null)
                        continue;
                    
                    // Write or delete a file on disk.
                    DistributedFile file = node.file;
                    switch( node.opType ) {
                        case( PUT ):
                            backup.saveFile( file.getName(), node.content, file.getVersion(), file.isDirectory(), file.getHintedHandoff(), true );
                            break;
                        case( DELETE ):
                            backup.deleteFile( file.getName(), file.getVersion(), file.isDirectory(), file.getHintedHandoff() );
                            break;
                        case( REMOVE ):
                            backup.removeFile( file );
                            break;
                    }
                }
                catch( InterruptedException | IOException e ) {
                    e.printStackTrace();
                }
            }
        }
        
        /**
         * Removes the first element from the queue.
        */
        private QueueNode dequeue() throws InterruptedException
        {
            lock.lock();
            
            while(filesQueue.isEmpty()) {
                if(shutDown.get()) { lock.unlock(); return null; }
                notEmpty.awaitNanos( 500000 ); // 0.5 seconds.
            }
            
            // Get and remove the first element of the queue.
            QueueNode node = filesQueue.remove( 0 );
            
            lock.unlock();
            
            return node;
        }
        
        /**
         * Inserts a file into the queue.
         * 
         * @param file     the file to add or remove.
         * @param content  the file's content. It may be {@code null} in case of a directory or in a delete or remove operation
         * @param opType   the operation to perform ({@link #PUT}, {@link #DELETE} or {@link #REMOVE})
        */
        public void enqueue( DistributedFile file, byte[] content, byte opType )
        {
            lock.lock();
            
            filesQueue.put( file.getName(), new QueueNode( file, content, opType ) );
            if(filesQueue.size() == 1)
                notEmpty.signal();
            
            lock.unlock();
        }
        
        
        
        private class QueueNode
        {
            public final DistributedFile file;
            public final byte[] content;
            public final byte opType;
            
            public QueueNode( DistributedFile file, byte[] content, byte opType )
            {
                this.file = file;
                this.content = content;
                this.opType = opType;
            }
        }
    }
}
