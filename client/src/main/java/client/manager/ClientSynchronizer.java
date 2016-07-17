/**
 * @author Stefano Ceccotti
*/

package client.manager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.versioning.Occurred;
import distributed_fs.versioning.VectorClock;

/**
 * Class used to synchronize the client
 * with a consistent view of
 * the distributed file system.
*/
public class ClientSynchronizer extends Thread
{
    private final DFSService service;
    private final DFSDatabase database;
    
    private final ReentrantLock lock;
    
    private static final Scanner SCAN = new Scanner( System.in );
    
    // Time to wait before to check the database (0.5 seconds).
    private static final int DB_CHECK_TIMER = 500;
    // Time to wait before to synchronize the client (30 seconds).
    private static final int SYNCH_TIMER = 30000;
    
    protected static final Logger LOGGER = Logger.getLogger( ClientSynchronizer.class );
    
    
    
    
    
    public ClientSynchronizer( final DFSService service,
                               final DFSDatabase database,
                               final ReentrantLock lock )
    {
        setName( "ClientSynchronizer" );
        
        this.service = service;
        this.database = database;
        
        this.lock = lock;
    }
    
    @Override
    public void run()
    {
        LOGGER.info( "ClientSynchronizer Thread launched." );
        
        final int tick = SYNCH_TIMER / DB_CHECK_TIMER;
        int count = tick;
        
        while(!service.isClosed()) {
            try {
                // First update the database.
                updateDB();
                
                if(service.isClosed())
                    break;
                
                if(count < tick)
                    count++;
                else {
                    // Then looks for updated versions in remote nodes.
                    count = 0;
                    List<DistributedFile> files = service.getAllFiles();
                    if(files != null)
                        checkFiles( files );
                }
            }
            catch( IOException | DFSException e ) {}
            
            try { sleep( DB_CHECK_TIMER ); }
            catch( InterruptedException e ) { break; }
        }
        
        LOGGER.info( "ClientSynchronizer Thread closed." );
    }
    
    /**
     * Reload the database looking for some files
     * that have to be updated.
    */
    private void updateDB() throws IOException, DFSException
    {
        database.loadFiles();
        List<String> toUpdate = database.getUpdateList();
        // All the new files are sent using a put request.
        for(String fileName : toUpdate)
            service.put( fileName );
        toUpdate.clear();
        
        /*String dbRoot = database.getFileSystemRoot();
        for(DistributedFile file : database.getAllFiles()) {
            lock.lock();
            
            File f = new File( dbRoot + file.getName() );
            if(file.isDeleted() || !f.exists())
                lock.unlock();
            else {
                long timestamp = file.lastModified();
                //System.out.println( "FILE: " + file.getName() +
                                    //", MY: " + timestamp + ", FILE: " + f.lastModified() );
                if(timestamp == 0) {
                    // Update its timestamp
                    database.updateLastModified( file.getName(), f.lastModified() );
                    //file.setLastModified( f.lastModified() );
                    lock.unlock();
                }
                else {
                    if(timestamp < f.lastModified()) {
                        // Update the file and send it using a put request.
                        database.updateLastModified( file.getName(), f.lastModified() );
                        //file.setLastModified( f.lastModified() );
                        lock.unlock();
                        service.put( file.getName() );
                    }
                }
            }
        }*/
    }
    
    /**
     * The downloaded files are merged with the own ones.
     * 
     * @param files   the list of files
    */
    public void checkFiles( final List<DistributedFile> files ) throws DFSException
    {
        for(DistributedFile file : files) {
            String fileName = file.getName();
            
            lock.lock();
            
            DistributedFile myFile = database.getFile( fileName );
            try {
                VectorClock clock = file.getVersion();
                if(myFile != null) clock = clock.merge( myFile.getVersion() );
                
                if(myFile == null || !reconcileVersions( myFile, file )) {
                    // The received file has the most updated version.
                    // Update the file on database.
                    if(!file.isDeleted())
                        database.saveFile( file, clock, null, true );
                    else
                        database.deleteFile( fileName, clock, file.isDirectory(), null );
                }
                else {
                    // If the deleted state of the own file is different than
                    // the received one, it's write-back.
                    if(myFile.isDeleted() != file.isDeleted()) {
                        if(myFile.isDeleted())
                            service.delete( fileName );
                        else
                            service.put( fileName );
                    }
                }
            }
            catch( IOException e ) {}
            
            lock.unlock();
        }
    }
    
    /**
     * Makes the reconciliation among different vector clocks.
     * 
     * @param myFile       the own file
     * @param otherFile    the received file
     * 
     * @return {@code true} if the own file has the most updated version,
     *         {@code false} otherwise
    */
    private boolean reconcileVersions( final DistributedFile myFile,
                                       final DistributedFile otherFile )
    {
        VectorClock myClock = myFile.getVersion();
        VectorClock otherClock = otherFile.getVersion();
        
        Occurred occ = myClock.compare( otherClock );
        if(occ == Occurred.AFTER) return true;
        if(occ == Occurred.BEFORE) return false;
        return makeReconciliation( Arrays.asList( myFile, otherFile ) ) == 0;
    }

    /**
     * Asks to the client which is the correct version.
     * 
     * @param versions    list of versions for the same file
     * 
     * @return Index of the selected file
    */
    public synchronized int makeReconciliation( final List<DistributedFile> versions )
    {
        int size = versions.size();
        if(size == 1)
            return 0;
        
        System.out.println( "There are multiple versions of the file '" + versions.get( 0 ).getName() + "', which are: " );
        for(int i = 0; i < size; i++)
            System.out.println( (i + 1) + ") " + versions.get( i ) );
        
        while(true) {
            System.out.print( "Choose the correct version: " );
            int id = SCAN.nextInt() - 1;
            if(id >= 0 && id < size)
                return id;
            else
                System.out.println( "Error: select a number in the range [1-" + size + "]" );
        }
    }
    
    public void shutDown()
    {
        interrupt();
    }
}
