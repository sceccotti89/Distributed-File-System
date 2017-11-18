package gossiping;

import java.io.Serializable;
import java.net.InetSocketAddress;

import org.json.JSONObject;

/**
 * An abstract class representing a gossip member.
*/
public abstract class GossipMember implements Comparable<GossipMember>, Serializable
{
	public static final String JSON_HOST = "host";
	public static final String JSON_PORT = "port";
	public static final String JSON_HEARTBEAT = "heartbeat";
	public static final String JSON_ID = "id";
	public static final String JSON_NODE_TYPE = "node_type";
	public static final String JSON_VNODES = "virtual_nodes";
	
	public static final int LOAD_BALANCER = 0, STORAGE = 1;
	
	protected final String _host;
	protected final int _port;
	protected int _heartbeat;
	protected int _nodeType;
	protected int _virtualNodes;

	/**
	 * The purpose of the id field is to be able for nodes to identify themselves beyond there host/port. For example
	 * an application might generate a persistent id so if they rejoin the cluster at a different host and port we 
	 * are aware it is the same node.
	*/
	protected String _id;

	/** Generated serial Id */
	private static final long serialVersionUID = -7393568021642370441L;

	/**
	 * Constructor.
	 * 
	 * @param host			The hostname or IP address.
	 * @param port			The port number.
	 * @param id			An id that may be replaced after contact.
	 * @param virtualNodes	Number of virtual nodes associated.
	 * @param nodeType		Type of the node (LoadBalancer or Storage node).
	 * @param heartbeat		The current heartbeat.
	*/
	public GossipMember( String host, int port, String id, int virtualNodes, int nodeType, int heartbeat ) 
	{
		_host = host;
		_port = port;
		_id = id;
		_heartbeat = heartbeat;
		_virtualNodes = virtualNodes;
		_nodeType = nodeType;
	}

	/**
	 * Get the hostname or IP address of the remote gossip member.
	 * @return The hostname or IP address.
	*/
	public String getHost() 
	{
		return _host;
	}

	/**
	 * Get the port number of the remote gossip member.
	 * @return The port number.
	*/
	public int getPort() 
	{
		return _port;
	}

	/**
	 * The member address in the form IP/host:port
	 * Similar to the toString in {@link InetSocketAddress}
	*/
	public String getAddress() 
	{
		return _host + ":" + _port;
	}
	
	/**
	 * Get the numer of virtual nodes of the remote gossip member.
	 * @return The virtual nodes.
	*/
	public int getVirtualNodes()
	{
		return _virtualNodes;
	}
	
	/**
	 * Set the number of virtual nodes.
	 * @param vNodes	
	*/
	public void setVirtualNodes( int vNodes )
	{
		_virtualNodes = vNodes;
	}
	
	/**
	 * Get the type of the remote gossip member.
	 * @return The node type.
	*/
	public int getNodeType()
	{
		return _nodeType;
	}

	/**
	 * Get the heartbeat of this gossip member.
	 * @return The current heartbeat.
	*/
	public int getHeartbeat() 
	{
		return _heartbeat;
	}

	/**
	 * Set the heartbeat of this gossip member.
	 * @param heartbeat The new heartbeat.
	*/
	public void setHeartbeat( int heartbeat ) 
	{
		this._heartbeat = heartbeat;
	}

	public String getId() 
	{
		return _id;
	}

	public void setId( String id ) 
	{
		_id = id;
	}

	@Override
	public String toString() 
	{
		return "Member [address=" + getAddress() + ", id=" + _id + ", heartbeat=" + _heartbeat + "]";
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		String address = getAddress();
		result = prime * result
		+ ((address == null) ? 0 : address.hashCode());
		return result;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals( Object obj ) 
	{
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			System.err.println( "equals(): obj is null." );
			return false;
		}
		
		if(obj instanceof GossipNode) {
		    return getAddress().equals( ((GossipNode) obj).getMember().getAddress() );
		}
		
		if (! (obj instanceof GossipMember) ) {
			System.err.println( "equals(): obj is not of type GossipMember." );
			return false;
		}
		
		// The object is the same if they both have the same address (hostname and port).
		return getAddress().equals( ((GossipMember) obj).getAddress() );
	}

	/**
	 * Get the JSONObject which is the JSON representation of this GossipMember.
	 * 
	 * @return The JSONObject of this GossipMember.
	 */
	public JSONObject toJSONObject() 
	{
		JSONObject jsonObject = new JSONObject();
		jsonObject.put( JSON_HOST, _host );
		jsonObject.put( JSON_PORT, _port );
		jsonObject.put( JSON_ID, _id );
		jsonObject.put( JSON_HEARTBEAT, _heartbeat );
		jsonObject.put( JSON_VNODES, _virtualNodes );
		jsonObject.put( JSON_NODE_TYPE, _nodeType );
		return jsonObject;
	}

	@Override
	public int compareTo( GossipMember other )
	{
		return this.getAddress().compareTo( other.getAddress() );
	}
}
