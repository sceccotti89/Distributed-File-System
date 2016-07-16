
package gossiping;

import javax.management.NotificationListener;

/**
 * This object represent a gossip member with the properties known locally.
 * These objects are stored in the local list of gossip members
 */
public class LocalGossipMember extends GossipMember
{
	/** The timeout timer for this gossip member. */
	private final transient GossipTimeoutTimer timeoutTimer;
	
	/** Generated serial id. */
	private static final long serialVersionUID = -1816709369882977145L;
	
	/**
	 * Constructor.
	 *
	 * @param hostname    			 The hostname or IP address.
	 * @param port        			 The port number.
	 * @param id					 
	 * @param nodeType				 
	 * @param virtualNodes			 
	 * @param heartbeat   			 The current heartbeat.
	 * @param notificationListener	 
	 * @param cleanupTimeout         The cleanup timeout for this gossip member.
	 */
	public LocalGossipMember( final String hostname, final int port, final String id,
							  final int virtualNodes, final int nodeType, final int heartbeat,
							  final NotificationListener notificationListener,
							  final int cleanupTimeout ) 
	{
		super( hostname, port, id, virtualNodes, nodeType, heartbeat );
		timeoutTimer = new GossipTimeoutTimer( cleanupTimeout, notificationListener, this );
	}
	
	/**
	 * Start the timeout timer.
	 */
	public void startTimeoutTimer() 
	{
		timeoutTimer.start();
	}
	
	/**
	 * Reset the timeout timer.
	 */
	public void resetTimeoutTimer() 
	{
		timeoutTimer.reset();
	}
}