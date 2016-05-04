/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.files.DistributedFile;
import distributed_fs.files.FileManagerThread;
import distributed_fs.net.NetworkMonitor;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.NodeStatistics;
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;
import gossiping.GossipRunner;
import gossiping.LogLevel;
import gossiping.event.GossipListener;
import gossiping.event.GossipState;

public abstract class DFSnode extends Thread implements GossipListener
{
	protected static String _address;
	protected int port;
	protected static NetworkMonitor monitor;
	protected static NodeStatistics stats;
	protected ExecutorService threadPool;
	protected TCPnet _net;
	protected FileManagerThread fMgr;
	protected ConsistentHasherImpl<GossipMember, String> cHasher;
	protected HashSet<String> filterAddress;
	protected GossipRunner runner;
	protected boolean shutDown = false;
	private static boolean testing = false;
	
	protected static final int WAIT_CLOSE = 2000;
	public static final Logger LOGGER = Logger.getLogger( DFSnode.class.getName() );
	
	public DFSnode( final int nodeType ) throws IOException, JSONException
	{
		setConfigure();
		Utils.createDirectory( Utils.RESOURCE_LOCATION );
		
		if(nodeType == GossipMember.STORAGE)
			QuorumSystem.init();
		
		//TODO LOGGER.setLevel( Utils.logLevel );
		
		cHasher = new ConsistentHasherImpl<>();
		stats = new NodeStatistics();
		_net = new TCPnet();
		threadPool = Executors.newCachedThreadPool();
		
		runner = new GossipRunner( new File( Utils.DISTRIBUTED_FS_CONFIG ), this, _address, Utils.computeVirtualNodes(), nodeType );
		
		Runtime.getRuntime().addShutdownHook( new Thread( new Runnable() 
		{
			@Override
			public void run() 
			{
				closeResources();
			}
		}));
	}
	
	/** Testing. */
	public DFSnode() throws IOException, JSONException
	{
		testing = true;
		
		Utils.createDirectory( Utils.RESOURCE_LOCATION );
		//TODO LOGGER.setLevel( Utils.logLevel );
		
		cHasher = new ConsistentHasherImpl<>();
		stats = new NodeStatistics();
		_net = new TCPnet();
		threadPool = Executors.newCachedThreadPool();
	}
	
	/**
	 * Constructor used to handle an instance of the Distributed node,
	 * due to an incoming request.
	*/
	public DFSnode( final TCPnet net,
					final FileManagerThread fMgr,
					final ConsistentHasherImpl<GossipMember, String> cHasher )
	{
		this._net = net;
		this.fMgr = fMgr;
		this.cHasher = cHasher;
		Utils.createDirectory( Utils.RESOURCE_LOCATION );
	}
	
	public String getAddress() {
		return _address;
	}
	
	public int getPort() {
		return port;
	}
	
	/**
	 * Get the statistics of the node.
	*/
	public NodeStatistics getStatistics()
	{
		return stats;
	}
	
	@Override
	public void gossipEvent( final GossipMember member, final GossipState state )
	{
		if(state == GossipState.DOWN) {
			LOGGER.info( "Removed node: " + member.toJSONObject().toString() );
			try{ cHasher.removeBucket( member ); }
			catch( InterruptedException e ){}
		}
		else {
			LOGGER.info( "Added node: " + member.toJSONObject().toString() );
			cHasher.addBucket( member, member.getVirtualNodes() );
			if(fMgr != null) {
				fMgr.getDatabase().checkMember( member.getHost() );
			}
		}
	}

	/** 
	 * Sets the initial configuration.
	*/
	private void setConfigure() throws IOException, JSONException
	{
		BasicConfigurator.configure();
		
		JSONObject file = Utils.parseJSONFile( Utils.DISTRIBUTED_FS_CONFIG );
		int logLevel = LogLevel.fromString( file.getString( "log_level" ) );
		Utils.logLevel = LogLevel.getLogLevel( logLevel );
		
		JSONArray inetwork = file.getJSONArray( "network_interface" );
		String inet = inetwork.getJSONObject( 0 ).getString( "inet" );
		int IPversion = inetwork.getJSONObject( 1 ).getInt( "IPversion" );
		
		_address = this.getNetworkAddress( inet, IPversion );
	}
	
	/** 
	 * Returns the network address of this machine.
	 * 
	 * @param inet			the address interface
	 * @param IPversion		the ip version (4 or 6)
	*/
	private String getNetworkAddress( final String inet, final int IPversion ) throws IOException
	{
		String _address = null;
		// enumerate all the network intefaces
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netInt : Collections.list( nets )) {
			LOGGER.debug( "NET: " + netInt.getName() );
			if(netInt.getName().equals( inet )) {
				// enumerate all the IP address associated with it
				for(InetAddress inetAddress : Collections.list( netInt.getInetAddresses() )) {
					LOGGER.debug( "ADDRESS: " + inetAddress );
					if(!inetAddress.isLoopbackAddress() &&
							((IPversion == 4 && inetAddress instanceof Inet4Address) ||
							(IPversion == 6 && inetAddress instanceof Inet6Address))) {
						_address = inetAddress.getHostAddress();
						if(inetAddress instanceof Inet6Address) {
							int index = _address.indexOf( '%' );
							_address = _address.substring( 0, index );
						}
						
						break;
					}
				}
			}
			
			if(_address != null)
				break;
		}
		
		if(_address == null)
			throw new IOException( "IP address not found: check your Internet connection or the configuration file " +
									Utils.DISTRIBUTED_FS_CONFIG );
		
		LOGGER.info( "Address: " + _address );
		
		return _address;
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	*/
	public DistributedFile getFile( String fileName ) {
		if(fMgr == null)
			return null;
		
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName = fileName + "/";
		
		DistributedFile file = fMgr.getDatabase().getFile( Utils.getId( fileName ) );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Returns the successor nodes of the input id.
	 * 
	 * @param id				source node identifier
	 * @param addressToRemove	the address to skip during the procedure
	 * @param numNodes			number of requested nodes
	 * 
	 * @return the list of successor nodes;
	 * 		   it could contains less than {@code numNodes} elements.
	*/
	protected List<GossipMember> getSuccessorNodes( final ByteBuffer id, final String addressToRemove, final int numNodes )
	{
		List<GossipMember> nodes = new ArrayList<>( numNodes );
		Set<String> filterAddress = new HashSet<>();
		int size = 0;
		
		if(!testing)
			filterAddress.add( addressToRemove );
		
		// choose the nodes whose address is different than this node
		ByteBuffer currId = id;
		while(size < numNodes) {
			currId = cHasher.getSuccessor( currId );
			if(currId == null)
				break;
			
			GossipMember node = cHasher.getBucket( currId );
			if(node != null && !filterAddress.contains( node.getHost() )) {
				nodes.add( node );
				if(!testing)
					filterAddress.add( node.getHost() );
				size++;
			}
		}
		
		currId = cHasher.getFirstKey();
		if(currId != null) {
			while(size < numNodes) {
				currId = cHasher.getSuccessor( currId );
				if(currId == null || currId.equals( id ))
					break;
				
				GossipMember node = cHasher.getBucket( currId );
				if(node != null && !filterAddress.contains( node.getHost() )) {
					nodes.add( node );
					if(!testing)
						filterAddress.add( node.getHost() );
					size++;
				}
			}
		}
		
		return nodes;
	}
	
	protected String getCodeString( final byte opType )
	{
		switch( opType ) {
			case( Message.GET ):	 return "GET";
			case( Message.PUT ):	 return "PUT";
			case( Message.DELETE ):	 return "DELETE";
			case( Message.GET_ALL ): return "GET_ALL";
		}
		
		return null;
	}
	
	/**
	 * Closes the opened resources.
	*/
	public void closeResources()
	{
		shutDown = true;
		
		try{ Thread.sleep( WAIT_CLOSE ); }
		catch( InterruptedException e ){}
		
		_net.close();
		monitor.shutDown();
		if(!testing)
			runner.getGossipService().shutdown();
		threadPool.shutdown();
		if(!testing && fMgr != null)
			fMgr.getDatabase().shutdown();
	}
}