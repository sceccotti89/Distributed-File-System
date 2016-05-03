/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.IOException;
import java.util.HashMap;

import distributed_fs.utils.Utils;

public class NetworkMonitorReceiverThread extends NetworkMonitor
{
	private final HashMap<String, NodeStatistics> nodes;
	
	public NetworkMonitorReceiverThread( final String address ) throws IOException
	{
		super( address );
		
		nodes = new HashMap<String, NodeStatistics>();
	}
	
	@Override
	public void run()
	{
		while(keepAlive.get()) {
			try {
				byte[] data = net.receiveMessage();
				//LOGGER.debug( "Received a message from " + net.getSrcAddress() );
				
				// save the statistics
				NodeStatistics stats = Utils.deserializeObject( data );
				nodes.put( net.getSrcAddress(), stats );
			} catch( Exception e ) {
				//e.printStackTrace();
			}
		}
	}
	
	@Override
	public NodeStatistics getStatisticsFor( final String address )
	{
		return nodes.get( address );
	}
}