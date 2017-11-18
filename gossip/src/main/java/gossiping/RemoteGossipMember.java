package gossiping;

import java.io.Serializable;

/**
 * The object represents a gossip member with the properties as received from a
 * remote gossip member.
 */
public class RemoteGossipMember extends GossipMember implements Serializable
{
	/** Generated serial Id */
	private static final long serialVersionUID = 7768966196098867797L;

	/**
	 * Constructor.
	 *
	 * @param hostname		The hostname or IP address.
	 * @param port      	The port number.
	 * @param id			
	 * @param virtualNodes	
	 * @param nodeType		
	 * @param heartbeat 	The current heartbeat.
	*/
	public RemoteGossipMember( String hostname, int port, String id, int virtualNodes, int nodeType, int heartbeat ) 
	{
		super( hostname, port, id, virtualNodes, nodeType, heartbeat );
	}

	/**
	 * Construct a RemoteGossipMember with a heartbeat of 0.
	 *
	 * @param hostname		The hostname or IP address.
	 * @param port          The port number.
	 * @param id			
	 * @param virtualNodes	
	 * @param nodeType		
	*/
	public RemoteGossipMember( String hostname, int port, String id, int virtualNodes, int nodeType ) 
	{
		super( hostname, port, id, virtualNodes, nodeType, 0 );
	}
}
