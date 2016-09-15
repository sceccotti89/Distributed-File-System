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
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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
    
    private final WeakHashMap<String, byte[]> cache;
    
    private final DataAccess dAccess = new DataAccess();
    
    protected final AtomicBoolean shutDown = new AtomicBoolean( false );
    
    private static final String RESOURCES_LOCATION = "Resources/";
    
    protected static final Logger LOGGER = Logger.getLogger( DBManager.class );
    
    
    
    
    
    public DBManager( final String resourcesLocation ) throws DFSException
    {
        cache = new WeakHashMap<>( 32 );
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
    protected String createResourcePath( final String path, final String defaultPath )
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
     * you could waits for all the pending operations for that particular file.
    */
    public void disableAsyncWrites()
    {
        if(!disableAsyncWrites) {
            asyncWriter.setClosed();
            disableAsyncWrites = true;
        }
    }
    
    /**
     * Enables the background Writer Thread.
     * 
     * @param enableParallelWorkers    setting it to {@code true} starts a number of threads that works on queue.
     *                                 It will speedup the queue operations, with a more expensive memory cost.
    */
    public void enableAsyncWrites( final boolean enableParallelWorkers )
    {
        if(disableAsyncWrites) {
            asyncWriter = new AsyncDiskWriter( this, enableParallelWorkers );
            disableAsyncWrites = false;
        }
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
    protected boolean createDirectory( final String dirPath ) {
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
    protected boolean createDirectory( final File dirFile )
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
    public boolean existFile( final String filePath, final boolean createIfNotExists ) throws IOException {
        return existFile( new File( filePath ), createIfNotExists );
    }
    
    /**
     * Checks whether a file exists.
     * 
     * @param file                  the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it doesn't exist,
     *                              {@code false} otherwise
    */
    protected boolean existFile( final File file, final boolean createIfNotExists ) throws IOException
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
     * Reads the serialization of a file from disk.
     * 
     * @param filePath      path to the file to read
     * 
     * @return the byte serialization of the object
    */
    public byte[] readFileFromDisk( final String filePath ) throws IOException
    {
        String fileName = root + normalizeFileName( filePath );
        
        if(!disableAsyncWrites && asyncWriter != null)
            asyncWriter.checkWrittenFile( fileName );
        
        DataLock dCond = dAccess.checkFileInUse( fileName, Message.GET );
        
        byte[] bytes = cache.get( filePath );
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
    public void writeFileOnDisk( final String filePath, final byte content[] ) throws IOException
    {
        if(content == null)
            createDirectory( filePath );
        else {
            String fileName = root + normalizeFileName( filePath );
            DataLock dCond = dAccess.checkFileInUse( fileName, Message.PUT );
            
            // Test whether the path to the file already exists.
            // If it not exists all the necessary directories are created.
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
    protected void deleteDirectory( final String dirPath ) {
        deleteDirectory( new File( dirPath ) );
    }
    
    /**
     * Deletes all the content of a directory.<br>
     * If it contains other folders inside, they will be deleted too
     * in a recursive manner.
     * 
     * @param dir  the current directory
    */
    protected void deleteDirectory( final File dir )
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
    protected void deleteFileOnDisk( final String filePath ) {
        deleteFileOnDisk( new File( filePath ) );
    }
    
    /** 
     * Deletes a file on disk.
     * 
     * @param file    file to remove
    */
    protected void deleteFileOnDisk( final File file )
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
    public void addListener( final DBListener listener )
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
    public void removeListener( final DBListener listener )
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
    protected void notifyListeners( final String fileName, final byte operation )
    {
        if(listeners != null) {
            for(DBListener listener : listeners)
                listener.dbEvent( fileName, operation );
        }
    }
    
    public static interface DBListener
    {
        public void dbEvent( final String fileName, final byte code );
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
        public final DataLock checkFileInUse( final String fileName, final byte opType )
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
        public final void notifyFileInUse( final String fileName, final DataLock dAccess )
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
            
            public DataLock( final byte opType )
            {
                this.opType = opType;
                queue = new ArrayDeque<>();
            }
            
            /**
             * Checks if the thread have to wait on queue,
             * depending on the operation type.
            */
            public void checkWaitOnQueue( final byte opType )
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
