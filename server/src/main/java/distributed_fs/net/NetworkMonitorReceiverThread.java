/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.IOException;
import java.util.HashMap;

import distributed_fs.utils.DFSUtils;

public class NetworkMonitorReceiverThread extends NetworkMonitorThread
{
	private final HashMap<String, NodeStatistics> nodes;
	
	public NetworkMonitorReceiverThread( final String address ) throws IOException
	{
		super( address );
		
		setDaemon( true );
		
		nodes = new HashMap<String, NodeStatistics>();
		net.setSoTimeout( 2000 );
	}
	
	@Override
	public void run()
	{
		while(keepAlive.get()) {
			try {
				byte[] data = net.receiveMessage();
				if(data == null)
					continue;
				
				//TODO LOGGER.debug( "Received a message from " + net.getSrcAddress() );
				
				// save the statistics
				NodeStatistics stats = DFSUtils.deserializeObject( decryptMessage( data ) );
				//TODO LOGGER.debug( "Stats: " + stats );
				nodes.put( net.getSrcAddress(), stats );
			}
			catch( Exception e ) {
				//e.printStackTrace();
			}
		}
		
		LOGGER.info( "Network Receiver closed." );
	}
	
	@Override
	public NodeStatistics getStatisticsFor( final String address )
	{
		return nodes.get( address );
	}
}