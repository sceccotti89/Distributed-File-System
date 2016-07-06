
package gossiping;

import java.net.UnknownHostException;
import java.util.List;

import org.apache.log4j.Logger;

import gossiping.event.GossipListener;
import gossiping.manager.GossipManager;
import gossiping.manager.random.RandomGossipManager;

/**
 * This object represents the service which is responsible for gossiping with
 * other gossip members.
 */
public class GossipService 
{
	public static final Logger LOGGER = Logger.getLogger( GossipService.class.getName() );

	private GossipManager _gossipManager;

	/**
	 * Constructor with the default settings.
	 *
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService( final StartupSettings startupSettings,
						  final GossipListener listener ) throws InterruptedException, UnknownHostException 
	{
		this( startupSettings.getAddress(),
			 startupSettings.getPort(),
			 startupSettings.getId(),
			 startupSettings.getVirtualNodes(),
			 startupSettings.getNodeType(),
			 startupSettings.getGossipMembers(),
			 startupSettings.getGossipSettings(),
			 listener
		);
	}

	/**
	 * Setup the client's lists, gossiping parameters, and parse the startup
	 * config file.
	 *
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public GossipService( final String ipAddress, final int port, final String id, final int virtualNodes, final int nodeType, 
						  final List<GossipMember> gossipMembers, final GossipSettings settings,
						  final GossipListener listener )
								  throws InterruptedException, UnknownHostException 
	{
		_gossipManager = new RandomGossipManager( ipAddress, port, id, virtualNodes, nodeType, settings, gossipMembers, listener );
	}

	public void start() 
	{
		_gossipManager.start();
	}

	public void shutdown() 
	{
		_gossipManager.shutdown();
	}

	public GossipManager getGossipManager() 
	{
		return _gossipManager;
	}

	public void setGossipManager( final GossipManager _gossipManager ) 
	{
		this._gossipManager = _gossipManager;
	}
}