/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class DBManager
{
    protected String root;
    
    private List<DBListener> listeners = null;
    
    protected boolean disableAsyncWrites = false;
    protected boolean disableReconciliation = false;
    
    private final ReentrantLock LOCK = new ReentrantLock( true );
    
    protected AtomicBoolean shutDown = new AtomicBoolean( false );
    
    public DBManager() {}
    
    /**
     * By default all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     * <p/>
     * It is possible to disable Background Writer Thread, but this greatly hurts concurrency.
     * Without async writes, all threads remain blocked until all previous writes are not finished (single big lock).
     *
     * <p/>
     * This may workaround some problems.
     *
    */
    public void disableAsyncWrites() {
        disableAsyncWrites = true;
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
    public String getFileSystemRoot()
    {
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
    protected boolean createDirectory( final String dirPath )
    {
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
        boolean created = false;
        
        LOCK.lock();
        
        if(dirFile.exists())
            created = true;
        else
            created = dirFile.mkdirs();
        
        LOCK.unlock();
        
        return created;
    }
    
    /**
     * Checks whether a file exists.
     * 
     * @param filePath              absolute or relative path to the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it shouldn't exists,
     *                              {@code false} otherwise
    */
    public boolean existFile( final String filePath, final boolean createIfNotExists ) throws IOException
    {
        return existFile( new File( filePath ), createIfNotExists );
    }
    
    /**
     * Checks whether a file exists.
     * 
     * @param file                  the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it shouldn't exists,
     *                              {@code false} otherwise
    */
    protected boolean existFile( final File file, final boolean createIfNotExists ) throws IOException
    {
        boolean exists = false;

        LOCK.lock();
        
        exists = file.exists();
        if(!exists && createIfNotExists) {
            if(file.getParent() != null)
                file.getParentFile().mkdirs();
            try { file.createNewFile(); }
            catch( IOException e ) { LOCK.unlock(); throw e; }
        }
        
        LOCK.unlock();
        
        return exists;
    }
    
    /**
     * Checks whether a file is a directory.
     * 
     * @param filePath      path to the file
    */
    protected boolean isDirectory( final String filePath )
    {
        boolean isDirectory = false;
        
        LOCK.lock();
        File file = new File( filePath );
        isDirectory = file.exists() && file.isDirectory();
        LOCK.unlock();
        
        return isDirectory;
    }
    
    /** 
     * Reads and serializes a file from disk.
     * 
     * @param filePath      path to the file to read
     * 
     * @return the byte serialization of the object
    */
    protected byte[] readFileFromDisk( String filePath ) throws IOException
    {
        filePath = root + normalizeFileName( filePath );
        
        LOCK.lock();
        
        File file = new File( filePath );
        
        int length = (int) file.length();
        byte bytes[] = new byte[length];
        
        FileInputStream fis = null;
        try { fis = new FileInputStream( file ); }
        catch( FileNotFoundException e ) { LOCK.unlock(); throw e; }
        
        BufferedInputStream bis = new BufferedInputStream( fis );
        
        try {
            bis.read( bytes, 0, bytes.length );
            
            bis.close();
            fis.close();
        }
        catch( IOException e ) {
            LOCK.unlock();
            throw e;
        }
        
        LOCK.unlock();
        
        return bytes;
    }
    
    /** 
     * Saves a file on disk.
     * 
     * @param filePath  path where the file have to be write
     * @param content   bytes of the serialized object
    */
    protected void saveFileOnDisk( final String filePath, final byte content[] ) throws IOException
    {
        if(content == null)
            createDirectory( filePath );
        else {
            LOCK.lock();
            
            // Test whether the path to that file doesn't exist.
            // In that case create all the necessary directories.
            File file = new File( filePath ).getParentFile();
            if(!file.exists())
                file.mkdirs();
            
            FileOutputStream fos = null;
            try { fos = new FileOutputStream( filePath ); }
            catch( FileNotFoundException e ){ LOCK.unlock(); throw e; }
            
            BufferedOutputStream bos = new BufferedOutputStream( fos );
            
            try {
                bos.write( content, 0, content.length );
                bos.flush();
                
                fos.close();
                bos.close();
            }
            catch( IOException e ) {
                LOCK.unlock();
                throw e;
            }
            
            LOCK.unlock();
        }
    }
    
    /**
     * Deletes all the content of a directory.<br>
     * If it contains other folders inside, them will be deleted too
     * in a recursive manner.
     * 
     * @param dir  the current directory
    */
    protected void deleteDirectory( final File dir )
    {
        LOCK.lock();
        
        if(dir.exists()) {
            for(File f : dir.listFiles()) {
                if(f.isDirectory())
                    deleteDirectory( f );
                f.delete();
            }
            
            dir.delete();
        }
        
        LOCK.unlock();
    }
    
    /** 
     * Deletes a file on disk.
     * 
     * @param file  the name of the file
    */
    protected void deleteFileOnDisk( final String file )
    {
        LOCK.lock();
        
        File f = new File( file );
        if(f.exists())
            f.delete();
        
        LOCK.unlock();
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
}