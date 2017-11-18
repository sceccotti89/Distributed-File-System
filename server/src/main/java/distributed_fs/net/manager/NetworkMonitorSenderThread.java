/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net.manager;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import distributed_fs.overlay.DFSNode;
import distributed_fs.utils.DFSUtils;

public class NetworkMonitorSenderThread extends NetworkMonitorThread
{
    private final DFSNode node;
    private final OperatingSystemMXBean system;
    
    private static final int SLEEP = 5000;
    
    public NetworkMonitorSenderThread( String address, DFSNode node ) throws IOException
    {
        super( address );
        setName( "NetworkMonitorSender" );
        
        this.node = node;
        this.system = ManagementFactory.getOperatingSystemMXBean();
    }
    
    @Override
    public void run()
    {
        while(keepAlive.get()) {
            try { Thread.sleep( SLEEP ); }
            catch ( InterruptedException e ) { break; }
            
            try {
                sendStatistics();
            }
            catch ( Exception e ) {
                //e.printStackTrace();
            }
        }
        
        LOGGER.info( "Network Sender closed." );
    }
    
    /**
     * Sends in UDP broadcast the current statistics.
    */
    private void sendStatistics() throws Exception
    {
        double loadAverage = system.getSystemLoadAverage();
        NodeStatistics stats = node.getStatistics();
        stats.setValue( NodeStatistics.WORKLOAD, loadAverage );
        
        //System.err.println( "WorkLoad: " + stats.getAverageLoad() );
        
        byte[] data = encryptMessage( DFSUtils.serializeObject( stats ) );
        net.sendMulticastMessage( data );
    }

    @Override
    public NodeStatistics getStatisticsFor( String address ) {
        return null;
    }
}
