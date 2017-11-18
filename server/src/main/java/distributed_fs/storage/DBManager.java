/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.map.LinkedMap;
import org.apache.log4j.Logger;

import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.storage.DBManager.DataAccess.DataLock;

public abstract class DBManager
{
    protected final String root;
    
    private List<DBListener> listeners = null;
    
    protected AsyncDiskWriter asyncWriter;
    
    private final ReentrantReadWriteLock DB_LOCK = new ReentrantReadWriteLock( true );
    protected final ReadLock LOCK_READERS = DB_LOCK.readLock();
    protected final WriteLock LOCK_WRITERS = DB_LOCK.writeLock();
    
    protected boolean disableAsyncWrites = false;
    protected boolean disableReconciliation = false;
    protected boolean disableScanDB = false;
    
    private final DBCache cache;
    
    private final DataAccess dAccess = new DataAccess();
    
    protected final AtomicBoolean launched = new AtomicBoolean( false );
    protected final AtomicBoolean shutDown = new AtomicBoolean( false );
    
    private static final String RESOURCES_LOCATION = "Resources/";
    
    protected static final Logger LOGGER = Logger.getLogger( DBManager.class );
    
    
    
    
    
    public DBManager( String resourcesLocation ) throws DFSException
    {
        cache = new DBCache();
        root = createResourcePath( resourcesLocation, RESOURCES_LOCATION );
        if(!createDirectory( root )) {
            throw new DFSException( "Invalid resources path " + root + ".\n" +
                                    "Make sure that the path is correct and that " +
                                    "you have the permissions to create and execute it." );
        }
        LOGGER.info( "Resources created on: " + root );
    }
    
    /**
     * Returns the "normalized" path to the resources.
     * 
     * @param path          the path to the resources
     * @param defaultPath   the default path if {@code path} is {@code null}
     * 
     * @return the normalized path
    */
    protected String createResourcePath( String path, String defaultPath )
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
     * By default all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     * <p/>
     * It is possible to disable background Writer Thread, but this greatly hurts concurrency.
     * Without async writes, all threads remain blocked until all previous writes are not finished (single big lock).
     *
     * <p/>
     * The main drawback rises during a disk-read request:
     * you could waits for all the pending operations for the requested file.
    */
    public void disableAsyncWrites()
    {
        if(!disableAsyncWrites) {
            if(asyncWriter != null) asyncWriter.setClosed();
            disableAsyncWrites = true;
        }
    }
    
    /**
     * Enables the background Writer Thread.
     * 
     * @param enableParallelWorkers    setting it to {@code true} starts a number of threads that works on queue.
     *                                 It will speedup the queue operations, with a more expensive memory cost.
    */
    public void enableAsyncWrites( boolean enableParallelWorkers )
    {
        if(disableAsyncWrites) {
            asyncWriter = new AsyncDiskWriter( this, enableParallelWorkers );
            disableAsyncWrites = false;
        }
    }
    
    /**
     * Disable the scanner database.
    */
    public void disableScanDB() {
        disableScanDB = true;
    }
    
    /**
     * Disable the conflict resolutor
     * when two files have concurrent versions.
    */
    public void disableResolveConflicts() {
        disableReconciliation = true;
    }
    
    /**
     * Returns the file system root location.
    */
    public String getFileSystemRoot() {
        return root;
    }
    
    /**
     * Creates a new directory, if it doesn't exist.
     * 
     * @param dirPath   path to the directory
     * 
     * @return {@code true} if the directory has been created,
     *         {@code false} otherwise.
    */
    protected boolean createDirectory( String dirPath ) {
        return createDirectory( new File( dirPath ) );
    }
    
    /**
     * Creates a new directory, if it doesn't exist.
     * 
     * @param dirFile   the directory file
     * 
     * @return {@code true} if the directory has been created,
     *         {@code false} otherwise.
    */
    protected boolean createDirectory( File dirFile )
    {
        if(dirFile == null || dirFile.getPath().equals( root ))
            return true;
        
        boolean created = false;
        String fileName = root + normalizeFileName( dirFile.getPath() );
        
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.PUT );
        
        if(dirFile.exists())
            created = true;
        else {
            created = createDirectory( dirFile.getParentFile() ) &&
                      dirFile.mkdir();
        }
        
        dAccess.notifyFileInUse( fileName, dCond );
        
        return created;
    }
    
    /**
     * Checks whether a file exists.
     * 
     * @param filePath              absolute or relative path to the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it shouldn't exists,
     *                              {@code false} otherwise
    */
    public boolean existFile( String filePath, boolean createIfNotExists ) throws IOException {
        return existFile( new File( filePath ), createIfNotExists );
    }
    
    /**
     * Checks whether a file exists.
     * 
     * @param file                  the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it doesn't exist,
     *                              {@code false} otherwise
    */
    protected boolean existFile( File file, boolean createIfNotExists ) throws IOException
    {
        boolean exists = false;
        String fileName = root + normalizeFileName( file.getPath() );
        
        if(!disableAsyncWrites && asyncWriter != null)
            asyncWriter.checkWrittenFile( fileName );
        
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.GET );
        
        exists = file.exists();
        if(!exists && createIfNotExists) {
            createDirectory( file.getParentFile() );
            try { file.createNewFile(); }
            catch( IOException e ) {
                dAccess.notifyFileInUse( fileName, dCond );
                throw e;
            }
        }
        
        dAccess.notifyFileInUse( fileName, dCond );
        
        return exists;
    }
    
    /**
     * Checks the existence of a file in the current file system,
     * starting from the root.
     * 
     * @param filePath  the file to search
     * 
     * @return {@code true} if the file is present,
     *         {@code false} otherwise
    */
    public boolean checkExistsInFileSystem( String filePath )
    {
        String rootPath = root + normalizeFileName( filePath );
        return checkExistsFile( new File( root ), rootPath );
    }
    
    private boolean checkExistsFile( File filePath, String fileName )
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
     * Reads the serialization of a file from disk.
     * 
     * @param filePath      path to the file to read
     * 
     * @return the byte serialization of the object
    */
    public byte[] readFileFromDisk( String filePath ) throws IOException
    {
        String fileName = root + normalizeFileName( filePath );
        
        if(!disableAsyncWrites && asyncWriter != null)
            asyncWriter.checkWrittenFile( fileName );
        
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.GET );
        
        byte[] bytes = cache.get( fileName );
        if(bytes == null) {
            File file = new File( fileName );
            FileInputStream fis = new FileInputStream( file );
            FileChannel fChannel = fis.getChannel();
            bytes = new byte[(int) file.length()];
            ByteBuffer bb = ByteBuffer.wrap( bytes );
            
            try {
                fChannel.read( bb );
                fChannel.close();
                fis.close();
            }
            catch( IOException e ) {
                dAccess.notifyFileInUse( fileName, dCond );
                throw e;
            }
            
            cache.put( fileName, bytes );
        }
        
        dAccess.notifyFileInUse( fileName, dCond );
        
        return bytes;
    }
    
    /** 
     * Writes a file on disk.
     * 
     * @param filePath  path where the file have to be write
     * @param content   bytes of the serialized object
    */
    public void writeFileOnDisk( String filePath, byte content[] ) throws IOException
    {
        if(content == null)
            createDirectory( filePath );
        else {
            String fileName = root + normalizeFileName( filePath );
            DataLock dCond = dAccess.checkFileInUse( fileName, Message.PUT );
            
            // Test whether the path to the file already exists.
            // If it doesn't exist all the necessary directories are created.
            File file = new File( fileName );
            File parent = file.getParentFile();
            if(parent != null && !parent.exists())
                createDirectory( parent );
            
            FileOutputStream out = new FileOutputStream( file );
            BufferedOutputStream bos = new BufferedOutputStream( out );
            
            try {
                bos.write( content, 0, content.length );
                bos.flush();
                bos.close();
            }
            catch( IOException e ) {
                dAccess.notifyFileInUse( fileName, dCond );
                throw e;
            }
            
            // Invalidate (if present) the version in the cache.
            cache.remove( fileName );
            
            dAccess.notifyFileInUse( fileName, dCond );
        }
    }
    
    /**
     * Deletes all the content of a directory.<br>
     * If it contains other folders inside, they will be deleted too
     * in a recursive manner.
     * 
     * @param dir  the current directory
    */
    protected void deleteDirectory( String dirPath ) {
        deleteDirectory( new File( dirPath ) );
    }
    
    /**
     * Deletes all the content of a directory.<br>
     * If it contains other folders inside, they will be deleted too
     * in a recursive manner.
     * 
     * @param dir  the current directory
    */
    protected void deleteDirectory( File dir )
    {
        String fileName = root + normalizeFileName( dir.getPath() );
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.DELETE );
        
        if(dir.exists()) {
            for(File f : dir.listFiles()) {
                if(f.isDirectory())
                    deleteDirectory( f );
                deleteFileOnDisk( f.getPath() );
            }
            
            dir.delete();
        }
        
        dAccess.notifyFileInUse( fileName, dCond );
    }
    
    /** 
     * Deletes a file on disk.
     * 
     * @param filePath  path to the file to delete
    */
    protected void deleteFileOnDisk( String filePath ) {
        deleteFileOnDisk( new File( filePath ) );
    }
    
    /** 
     * Deletes a file on disk.
     * 
     * @param file    file to remove
    */
    protected void deleteFileOnDisk( File file )
    {
        String fileName = root + normalizeFileName( file.getPath() );
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.DELETE );
        
        if(file.exists())
            file.delete();
        
        dAccess.notifyFileInUse( fileName, dCond );
    }
    
    /**
     * Checks if the file has the correct format.<br>
     * The normalization consists in removing all the
     * parts of the name that don't permit it to be independent
     * with respect to the database location.<br>
     * The returned string is a generic version of it.
     * 
     * @param fileName  the input file
     * 
     * @return the normalized file name
    */
    public String normalizeFileName( String fileName )
    {
        fileName = fileName.replace( "\\", "/" );
        String backup = fileName;
        if(fileName.startsWith( "./" ))
            fileName = fileName.substring( 2 );
        
        fileName = new File( fileName ).getAbsolutePath().replace( "\\", "/" );
        if(fileName.startsWith( root ))
            fileName = fileName.substring( root.length() );
        else
            fileName = backup;
        
        return fileName;
    }
    
    /**
     * Adds a new database listener.
    */
    public void addListener( DBListener listener )
    {
        if(listener != null) {
            if(listeners == null)
                listeners = new ArrayList<>();
            listeners.add( listener );
        }
    }
    
    /**
     * Removes the given listener.
    */
    public void removeListener( DBListener listener )
    {
        if(listener != null && listeners != null)
            listeners.remove( listener );
    }
    
    /**
     * Notifies all the attached listeners about the last operation done.
     * 
     * @param fileName     name of the file
     * @param operation    type of operation ({@code GET} or {@code DELETE})
    */
    protected void notifyListeners( String fileName, byte operation )
    {
        if(listeners != null) {
            for(DBListener listener : listeners)
                listener.dbEvent( fileName, operation );
        }
    }
    
    public static interface DBListener
    {
        public void dbEvent( String fileName, byte code );
    }
    
    /**
     * Class used to implement a cache.<br>
     * It manages all the most recent files read from the application.
    */
    private static final class DBCache
    {
        private final LinkedMap<String, byte[]> files;
        private int spaceOccupancy = 0;
        
        private static final int MAX_SIZE = 1 << 21; // 64MBytes
        
        
        
        public DBCache()
        {
            files = new LinkedMap<>( 64 );
        }
        
        /**
         * Associates the specified value with the specified key in this map
         * (optional operation).  If the map previously contained a mapping for
         * the key, the old value is replaced by the specified value.  (A map
         * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
         * if {@link #containsKey(Object) m.containsKey(k)} would return
         * <tt>true</tt>.)
         *
         * @param key key with which the specified value is to be associated
         * @param value value to be associated with the specified key
         * @return the previous value associated with <tt>key</tt>, or
         *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
         *         (A <tt>null</tt> return can also indicate that the map
         *         previously associated <tt>null</tt> with <tt>key</tt>,
         *         if the implementation supports <tt>null</tt> values.)
        */
        private void put( String key, byte[] value )
        {
            if(value == null)
                return;
            
            int length = value.length;
            if(length > MAX_SIZE) // Can't exceed the maximum size.
                return;
            
            while(length > getLeftSpace()) {
                // Remove the oldest entry in the cache.
                byte[] data = files.remove( 0 );
                spaceOccupancy -= data.length;
            }
            
            files.put( key, value );
            spaceOccupancy += length;
        }
        
        /**
         * Removes the mapping for a key from this map if it is present
         * (optional operation).   More formally, if this map contains a mapping
         * from key <tt>k</tt> to value <tt>v</tt> such that
         * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
         * is removed.  (The map can contain at most one such mapping.)
         *
         * <p>Returns the value to which this map previously associated the key,
         * or <tt>null</tt> if the map contained no mapping for the key.
         *
         * @param key key whose mapping is to be removed from the map
         * @return the previous value associated with <tt>key</tt>, or
         *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
        */
        private void remove( String key )
        {
            byte[] val = files.remove( key );
            if(val != null)
                spaceOccupancy -= val.length;
        }
        
        /**
         * Returns the value to which the specified key is mapped,
         * or {@code null} if this map contains no mapping for the key.
         *
         * <p>More formally, if this map contains a mapping from a key
         * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
         * key.equals(k))}, then this method returns {@code v}; otherwise
         * it returns {@code null}.  (There can be at most one such mapping.)
         *
         * @param key the key whose associated value is to be returned
         * @return the value to which the specified key is mapped, or
         *         {@code null} if this map contains no mapping for the key
        */
        private byte[] get( String key )
        {
            // Remove the node and put it in the bottom of the list.
            byte[] data = files.remove( key );
            if(data != null)
                files.put( key, data );
            
            return data;
        }
        
        /**
         * Returns the free space
         * used to store the files.
        */
        private int getLeftSpace() {
            return MAX_SIZE - spaceOccupancy;
        }
    }
    
    /**
     * Class used to access in safe mode the files
     * present on disk and on database as well.<br>
     * Based on the operation's type the file can be accessed
     * simultaneously by different threads (read) or in a
     * mutually exclusive way (write).<br>
     * The behaviour of the access follow the reader-writer
     * problem, namely N readers or only 1 writer at a time.
    */
    protected final class DataAccess
    {
        private final ReentrantLock dataLock = new ReentrantLock( true );
        private final Map<String, DataLock> filesInUse;
        
        public DataAccess()
        {
            filesInUse = new HashMap<>( 64 );
        }
        
        /**
         * Checks whether a file can be accessed or not.
         * 
         * @param fileName    name of the file
         * @param opType      type of operation
         * 
         * @return the object needed to access the file
        */
        public final DataLock checkFileInUse( String fileName, byte opType )
        {
            dataLock.lock();
            
            DataLock dAccess = filesInUse.get( fileName );
            if(dAccess == null)
                filesInUse.put( fileName, dAccess = new DataLock( opType ) );
            else
                dAccess.checkWaitOnQueue( opType );
            
            dataLock.unlock();
            
            return dAccess;
        }
        
        /**
         * When all the Threads responsible for the given file have finished, all the
         * Threads waiting on queue will be notified and awakened.
         * 
         * @param fileName    name of the file
         * @param dAccess     object used to access the file
        */
        public final void notifyFileInUse( String fileName, DataLock dAccess )
        {
            dataLock.lock();
            
            dAccess.wakeUpQueue();
            if(dAccess.isFinished())
                filesInUse.remove( fileName );
            
            dataLock.unlock();
        }
        
        public class DataLock
        {
            private byte opType;
            private int activeFlows = 1;
            private Deque<DataAccessCondition> queue;
            
            public DataLock( byte opType )
            {
                this.opType = opType;
                queue = new ArrayDeque<>();
            }
            
            /**
             * Checks if the thread have to wait on queue,
             * depending on the operation type.
            */
            public void checkWaitOnQueue( byte opType )
            {
                if(this.opType == Message.GET && this.opType == opType)
                    activeFlows++;
                else {
                    DataAccessCondition dCond = new DataAccessCondition();
                    dCond.opType = opType;
                    queue.addLast( dCond );
                    try { dCond.cond.await(); }
                    catch( InterruptedException e ) {}
                }
            }
            
            /**
             * Wakes up all the threads that are waiting on queue.
            */
            public void wakeUpQueue()
            {
                if(--activeFlows == 0) {
                    // All the active flows are finished.
                    // The waiting threads can be activated.
                    int size = queue.size();
                    for(int i = 0; i < size; i++) {
                        DataAccessCondition dCond = queue.removeFirst();
                        this.opType = dCond.opType;
                        dCond.cond.signal();
                        if(dCond.opType != Message.GET)
                            break;
                    }
                }
            }
            
            public boolean isFinished() {
                return activeFlows == 0 && queue.isEmpty();
            }
            
            private class DataAccessCondition
            {
                public byte opType;
                public Condition cond = dataLock.newCondition();
            }
        }
    }
}
