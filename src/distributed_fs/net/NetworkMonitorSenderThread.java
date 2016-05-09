/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import distributed_fs.overlay.DFSnode;
import distributed_fs.utils.Utils;

public class NetworkMonitorSenderThread extends NetworkMonitor
{
	private final DFSnode node;
	private final OperatingSystemMXBean system;
	
	private static final int SLEEP = 5000;
	
	public NetworkMonitorSenderThread( final String address, final DFSnode node ) throws IOException
	{
		super( address );
		
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
				double loadAverage = system.getSystemLoadAverage();
				NodeStatistics stats = node.getStatistics();
				stats.increaseValue( NodeStatistics.NUM_CONNECTIONS );
				stats.setValue( NodeStatistics.WORKLOAD, loadAverage );
				
				//System.err.println( "WorkLoad: " + stats.getAverageLoad() );
				
				byte[] data = encryptMessage( Utils.serializeObject( stats ) );
				//System.out.println( "DATA: " + Utils.deserializeObject( decryptMessage( data ) ) );
				//sendMessage( encryptMessage( data ) );
				net.sendMulticastMessage( data );
			}
			catch ( Exception e ) {
				//e.printStackTrace();
			}
		}
	}

	@Override
	public NodeStatistics getStatisticsFor( final String address ) {
		return null;
	}
}