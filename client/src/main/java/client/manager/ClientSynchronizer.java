/**
 * @author Stefano Ceccotti
*/

package client.manager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
    
    private static boolean reconciliation = false;
    
    private static final Scanner SCAN = new Scanner( System.in );
    
    // Time to wait before to check the database (0.5 seconds).
    private static final int DB_CHECK_TIMER = 500;
    // Time to wait before to synchronize the client (30 seconds).
    private static final int SYNCH_TIMER = 30000;
    
    protected static final Logger LOGGER = Logger.getLogger( ClientSynchronizer.class );
    
    
    
    
    
    public ClientSynchronizer( final DFSService service,
                               final DFSDatabase database )
    {
        setName( "ClientSynchronizer" );
        
        this.service = service;
        this.database = database;
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
            catch( IOException | DFSException e ) {
                e.printStackTrace();
            }
            
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
            
            DistributedFile myFile = database.getFile( fileName );
            try {
                VectorClock clock = file.getVersion();
                if(myFile != null){
                    Occurred occ = reconcileVersions( myFile, file );
                    if(occ != Occurred.AFTER && occ != Occurred.EQUALS) {
                        DistributedFile finalVersion;
                        if(occ == Occurred.CONCURRENTLY) {
                            // Ask the client for the version.
                            List<DistributedFile> vFiles = Arrays.asList( myFile, file );
                            int id = makeReconciliation( vFiles );
                            finalVersion = vFiles.get( id );
                            clock = clock.merge( myFile.getVersion() );
                        }
                        else {
                            // The input file has the most updated version.
                            finalVersion = file;
                        }
                        
                        // Update the file on database and
                        // write-back the reconciled version.
                        if(!finalVersion.isDeleted()) {
                            database.saveFile( finalVersion, clock, null, true );
                            service.put( fileName );
                        }
                        else {
                            service.delete( fileName );
                            //database.deleteFile( fileName, clock, file.isDirectory(), null );
                        }
                    }
                }
                else {
                    // The client doesn't own a version of the file.
                    // Update the file on database.
                    if(!file.isDeleted())
                        database.saveFile( file, clock, null, true );
                    else
                        database.deleteFile( fileName, clock, file.isDirectory(), null );
                }
            }
            catch( IOException e ) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Checks the occurred version of the files.
     * 
     * @param myFile       the own file
     * @param otherFile    the received file
     * 
     * @return the occurred version between the two files
    */
    private Occurred reconcileVersions( final DistributedFile myFile,
                                        final DistributedFile otherFile )
    {
        VectorClock myClock = myFile.getVersion();
        VectorClock otherClock = otherFile.getVersion();
        return myClock.compare( otherClock );
    }

    /**
     * Asks to the client which is the correct version.
     * 
     * @param versions    list of versions for the same file
     * 
     * @return Index of the selected file
    */
    public static synchronized int makeReconciliation( final List<DistributedFile> versions )
    {
        reconciliation = true;
        
        int size = versions.size();
        if(size == 1) {
            reconciliation = false;
            return 0;
        }
        
        System.out.println( "There are multiple versions of the file '" + versions.get( 0 ).getName() + "', which are: " );
        for(int i = 0; i < size; i++)
            System.out.println( (i + 1) + ") " + versions.get( i ) );
        
        while(true) {
            System.out.print( "Choose the correct version: " );
            int id = SCAN.nextInt() - 1;
            if(id >= 0 && id < size) {
                reconciliation = false;
                return id;
            }
            else
                System.out.println( "Error: select a number in the range [1-" + size + "]" );
        }
    }
    
    /**
     * Gets the reconciliation attribute.
     * 
     * @return {@code true} if the client is in the reconciliation phase,
     *         {@code false} otherwise
    */
    public boolean getReconciliation() {
        return reconciliation;
    }
    
    /**
     * Closes the thread.
     * 
     * @param waitTermination   {@code true} if the calling thread must wait for the termination,
     *                          {@code false} otherwise
    */
    public void shutDown( final boolean waitTermination )
    {
        interrupt();
        if(waitTermination) {
            try{ join(); }
            catch( InterruptedException e ) { e.printStackTrace(); }
        }
    }
}
