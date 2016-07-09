
package client.manager;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.RemoteFile;
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
    
    private static final Scanner SCAN = new Scanner( System.in );
    
    // Time to wait before to check the database (30 seconds).
    private static final int CHECK_TIMER = 30000;
    
    public ClientSynchronizer( final DFSService service, final DFSDatabase database )
    {
        this.service = service;
        this.database = database;
    }
    
    @Override
    public void run()
    {
        while(!service.isClosed()) {
            try {
                List<RemoteFile> files = service.getAllFiles();
                if(files != null)
                    checkFiles( files );
            }
            catch( DFSException e ){}
            
            try { sleep( CHECK_TIMER ); }
            catch( InterruptedException e ) { break; }
        }
    }
    
    /**
     * The downloaded files are merged with the own ones.
     * 
     * @param files   the list of files
    */
    public void checkFiles( final List<RemoteFile> files ) throws DFSException
    {
        for(RemoteFile file : files) {
            DistributedFile myFile = database.getFile( file.getName() );
            try {
                if(myFile == null ||
                   reconcileVersions( file.getName(), myFile.getVersion(), file.getVersion() ) == 1) {
                    // The received file has the most updated version.
                    // Update the file on database.
                    if(!file.isDeleted())
                        database.saveFile( file, file.getVersion(), null );
                    else
                        database.deleteFile( file.getName(), file.getVersion(), null );
                }
                else {
                    // The own file has the most updated version.
                    // Send this version using the put method.
                    service.put( myFile.getName() );
                }
            }
            catch( IOException e ) {}
        }
    }
    
    /**
     * Makes the reconciliation among different vector clocks.
     * 
     * @param fileName     name of the associated file
     * @param myClock      clock associated to the own file
     * @param otherClock   clock associated to the received file
     * 
     * @return Index of the selected file
    */
    private int reconcileVersions( final String fileName,
                                   final VectorClock myClock,
                                   final VectorClock otherClock )
    {
        Occurred occ = myClock.compare( otherClock );
        if(occ == Occurred.AFTER) return 0;
        if(occ == Occurred.BEFORE) return 1;
        return makeReconciliation( fileName, Arrays.asList( myClock, otherClock ) );
    }

    /**
     * Asks to the client which is the correct version.
     * 
     * @param fileName  the name of the file
     * @param clocks    list of vector clocks
     * 
     * @return Index of the selected file
    */
    public synchronized int makeReconciliation( final String fileName, final List<VectorClock> clocks )
    {
        int size = clocks.size();
        if(size == 1)
            return 0;
        
        System.out.println( "There are multiple versions of the file '" + fileName + "', which are: " );
        for(int i = 0; i < size; i++)
            System.out.println( (i + 1) + ") " + clocks.get( i ) );
        
        while(true) {
            System.out.print( "Choose the correct version: " );
            int id = SCAN.nextInt() - 1;
            if(id >= 0 && id < size)
                return id;
            else
                System.out.println( "Error: select a number in the range [1-" + size + "]" );
        }
    }
}
