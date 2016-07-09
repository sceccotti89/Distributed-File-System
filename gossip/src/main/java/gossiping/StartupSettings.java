package gossiping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This object represents the settings used when starting the gossip service.
 */
public class StartupSettings
{
	/** The port to start the gossip service on. */
	private int _port;
	/** The identifier used by the node of this machine */
	private String _id;
	/** The address used by the node of this machine */
	private String _address;
	/** The number of virtual nodes managed by this machine */
	private int _virtualNodes;
	/** The node type */
	private int _nodeType;
	/** The logging level of the gossip service. */
	private int _logLevel;
	/** The gossip settings used at startup. */
	private final GossipSettings _gossipSettings;
	/** The list with gossip members to start with. */
	private final List<GossipMember> _gossipMembers;

	/**
	 * Constructor.
	 *
	 * @param port      The port to start the service on.
	*/
	public StartupSettings( final String _address, final int port, final String id, final int virtualNodes,
							final int nodeType, final int logLevel ) 
	{
		this( _address, port, id, virtualNodes, nodeType, logLevel, new ArrayList<>(), new GossipSettings() );
	}
	
	/**
     * Constructor.
     *
     * @param port      The port to start the service on.
    */
	public StartupSettings( final String _address, final int port, final String id, final int virtualNodes,
                            final int nodeType, final int logLevel, final List<GossipMember> members )
	{
	    this( _address, port, id, virtualNodes, nodeType, logLevel, members, new GossipSettings() );
    }

	/**
	 * Constructor.
	 *
	 * @param port		The port to start the service on.
	*/
	public StartupSettings( final String address, final int port, final String id, final int virtualNodes,
							final int nodeType, final int logLevel, final List<GossipMember> members, final GossipSettings gossipSettings ) 
	{
		_address = address;
		_port = port;
		_id = id;
		_virtualNodes = virtualNodes;
		_nodeType = nodeType;
		_logLevel = logLevel;
		_gossipSettings = gossipSettings;
		_gossipMembers = members;
	}

	/**
	 * Set the port of the gossip service.
	 *
	 * @param port		The port for the gossip service.
	*/
	public void setPort( final int port ) 
	{
		_port = port;
	}

	/**
	 * Get the port for the gossip service.
	 *
	 * @return The port of the gossip service.
	*/
	public int getPort() 
	{
		return _port;
	}
	
	/**
	 * Get the id of the gossip service.
	 *
	 * @return The identifier of the gossip service.
	*/
	public String getId()
	{
		return _id;
	}
	
	/**
	 * Get the number of virtual nodes associated to the peer.
	 * 
	 * @return the virtual nodes
	*/
	public int getVirtualNodes()
	{
		return _virtualNodes;
	}

	/**
	 * Get the type of the node (LOAD_BALANCER or STORAGE_NODE)
	 * 
	 * @return the node type
	*/
	public int getNodeType()
	{
		return _nodeType;
	}
	
	/**
	 * Get the address of the gossip service.
	 *
	 * @return The address of the gossip service.
	*/
	public String getAddress()
	{
		return _address;
	}

	/**
	 * Set the log level of the gossip service.
	 *
	 * @param logLevel	The log level({LogLevel}).
	*/
	public void setLogLevel( final int logLevel ) 
	{
		_logLevel = logLevel;
	}

	/**
	 * Get the log level of the gossip service.
	 *
	 * @return The log level.
	*/
	public int getLogLevel() 
	{
		return _logLevel;
	}

	/**
	 * Get the GossipSettings.
	 *
	 * @return The GossipSettings object.
	*/
	public GossipSettings getGossipSettings() 
	{
		return _gossipSettings;
	}

	/**
	 * Add a gossip member to the list of members to start with.
	 *
	 * @param member	The member to add.
	*/
	public void addGossipMember( final GossipMember member ) 
	{
		_gossipMembers.add( member );
	}

	/**
	 * Get the list with gossip members.
	 *
	 * @return The gossip members.
	*/
	public List<GossipMember> getGossipMembers() 
	{
		return _gossipMembers;
	}

	/**
	 * Parse the settings for the gossip service from a JSON file.
	 *
	 * @param jsonFile	The file object which refers to the JSON config file.
	 * @param _address	IP address of this machine
	 * @param nodeType	Type of the node.
	 * 
	 * @return The StartupSettings object with the settings from the config file.
	 * 
	 * @throws JSONException			Thrown when the file is not a well-formed JSON.
	 * @throws FileNotFoundException	Thrown when the file cannot be found.
	 * @throws IOException	            Thrown when reading the file gives problems.
	*/
	public static StartupSettings fromJSONFile( final File jsonFile, final String _address, final int virtualNodes, final int nodeType )
			throws JSONException, FileNotFoundException, IOException
	{
		BufferedReader file = new BufferedReader( new FileReader( jsonFile ) );
		StringBuilder content = new StringBuilder( 512 );
		String line;
		while((line = file.readLine()) != null)
			content.append( line.trim() );
		file.close();
		
		JSONObject gossiping = new JSONObject( content.toString() );
		
		// Get the port number.
		int port = gossiping.getInt( "port" );
		// Get the log level from the config file.
		int logLevel = LogLevel.fromString( gossiping.getString( "log_level" ) );
		// Get the gossip_interval from the config file.
		int gossipInterval = gossiping.getInt( "gossip_interval" );
		// Get the cleanup_interval from the config file.
		int cleanupInterval = gossiping.getInt( "cleanup_interval" );
		
		System.out.println( "Config [port: " + port + ", log_level: " + logLevel + ", gossip_interval: " + gossipInterval +
							", cleanup_interval: " + cleanupInterval + "]" );
		
		//String id = DatatypeConverter.printHexBinary( getNodeId( 1, _address + ":" + port ) );
		
		GossipService.LOGGER.setLevel( LogLevel.getLogLevel( logLevel ) );
		
		// Initiate the settings with the port number.
		StartupSettings settings = new StartupSettings( _address, port, "", virtualNodes, nodeType, logLevel,
		                                                new ArrayList<>(),
														new GossipSettings( gossipInterval, cleanupInterval ) );
		
		// Now iterate over the members from the config file and add them to the settings.
		System.out.print( "Config-members [" );
		JSONArray membersJSON = gossiping.getJSONArray( "members" );
		int length = membersJSON.length();
		for(int i = 0; i < length; i++) {
			JSONObject memberJSON = membersJSON.getJSONObject( i );
			String host = memberJSON.getString( "host" );
			int Port = memberJSON.getInt( "port" );
			//id = DatatypeConverter.printHexBinary( getNodeId( 1, host + ":" + Port ) );
			RemoteGossipMember member = new RemoteGossipMember( host, Port, "", 0, memberJSON.getInt( "type" ) );
			settings.addGossipMember( member );
			System.out.print( member.getAddress() );
			if (i < (membersJSON.length() - 1))
				System.out.print( ", " );
		}
		System.out.println( "]" );
		
		// Return the created settings object.
		return settings;
	}
	
	/**
	 * Returns the identifier associated to the node.
	 * 
	 * @param virtualNode	the virtual node instance
	 * @param host			the host address
	 * 
	 * @return identifier in a byte array representation
	*/
	/*private static byte[] getNodeId( final int virtualNode, final String host )
	{
		byte[] hostInBytes = host.getBytes( StandardCharsets.UTF_8 );
		ByteBuffer bb = ByteBuffer.allocate( Integer.BYTES + hostInBytes.length );
		bb.putInt( virtualNode );
		bb.put( hostInBytes );
		
		return _hash.hashBytes( bb.array() );
	}*/
}
