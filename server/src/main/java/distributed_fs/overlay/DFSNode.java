/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.manager.NetworkMonitorThread;
import distributed_fs.net.manager.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.overlay.manager.FileTransferThread;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadMonitor.ThreadState;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.resources.ResourceLoader;
import gossiping.GossipMember;
import gossiping.GossipRunner;
import gossiping.RemoteGossipMember;
import gossiping.event.GossipListener;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public abstract class DFSNode extends Thread implements GossipListener
{
	protected static String _address;
	protected int port;
	
	protected static volatile NodeStatistics stats;
	protected TCPnet _net;
	
	private int vNodes;
    protected volatile ConsistentHasher<GossipMember, String> cHasher;
	protected Set<String> filterAddress;
	protected GossipManager gManager;
	
	protected ExecutorService threadPool;
	protected NetworkMonitorThread netMonitor;
	protected ThreadMonitor monitor_t;
	protected volatile FileTransferThread fMgr;
	
	protected boolean startGossiping = true;
	protected AtomicBoolean shutDown = new AtomicBoolean( false );
	
	protected ThreadState state;
	// Actions performed by the thread, during the request processing.
	protected Deque<Object> actionsList;
	
	protected long id;
	protected boolean completed = false; // Used to check if the thread has completed the job.
	private long nextThreadID; // Next unique identifier associated with the Thread.
	
	// Used to create the list of actions done by the node.
    public static final Object DONE = new Object();
	
	protected static final int MAX_USERS = 64; // Maximum number of accepted connections.
	public static final int WAIT_CLOSE = 200;
	public static final Logger LOGGER = Logger.getLogger( DFSNode.class.getName() );
	
	// Configuration path.
    private static final String DISTRIBUTED_FS_CONFIG = "Settings/NodeSettings.json";
    
    public static final int PORT_OFFSET = 1;
    
	
	
	
	public DFSNode( final String address,
					final int port,
					final int virtualNodes,
					final int nodeType,
	                final List<GossipMember> startupMembers ) throws IOException
	{
		_address = address;
		this.port = (port <= 0) ? GossipManager.GOSSIPING_PORT : port;
		
		setConfigure();
		
		cHasher = new ConsistentHasherImpl<>();
		stats = new NodeStatistics();
		_net = new TCPnet();
		_net.setSoTimeout( WAIT_CLOSE );
		
		//threadPool = Executors.newCachedThreadPool();
		threadPool = Executors.newFixedThreadPool( MAX_USERS );
		
		GossipRunner runner;
		if(startupMembers == null || startupMembers.size() == 0)
			runner = new GossipRunner( DFSUtils.GOSSIP_CONFIG, this, _address, this.port, virtualNodes, nodeType );
		else
		    runner = new GossipRunner( this, _address, this.port, DFSUtils.getNodeId( 1, _address ), virtualNodes, nodeType, startupMembers );
		gManager = runner.getGossipService().getGossipManager();
		
		vNodes = gManager.getVirtualNodes();
		
		Runtime.getRuntime().addShutdownHook( new Thread( new Runnable() 
		{
			@Override
			public void run() 
			{
				close();
				LOGGER.info( "Service has been shutdown..." );
			}
		}));
	}
	
	/**
	 * Constructor used to handle an instance of the Distributed node,
	 * due to an incoming request.
	*/
	public DFSNode( final TCPnet net,
					final FileTransferThread fMgr,
					final ConsistentHasher<GossipMember, String> cHasher )
	{
		this._net = net;
		this.fMgr = fMgr;
		this.cHasher = cHasher;
	}
	
	/**
     * Enable/disable the gossiping mechanism.<br>
     * By default this value is setted to {@code true}.
     * 
     * @param enable    {@code true} to enable the anti-entropy mechanism,
     *                  {@code false} otherwise
    */
    public void setGossipingMechanism( final boolean enable )
    {
        if(!isAlive())
            startGossiping = enable;
    }
    
    /**
     * Gets the list of members present in the configuration file.
     * 
     * @param configFile    the configuration file
    */
    protected static List<GossipMember> getStartupMembers( final JSONObject configFile )
    {
        List<GossipMember> members = null;
        if(!configFile.has( "members" ))
            return members;
        
        JSONArray membersJSON = configFile.getJSONArray( "members" );
        int length = membersJSON.length();
        members = new ArrayList<>( length );
        
        for(int i = 0; i < length; i++) {
            JSONObject memberJSON = membersJSON.getJSONObject( i );
            String host = memberJSON.getString( "host" );
            int Port = memberJSON.getInt( "port" );
            RemoteGossipMember member = new RemoteGossipMember( host, Port, "", 0, memberJSON.getInt( "type" ) );
            members.add( member );
        }
        
        return members;
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
			LOGGER.info( "Removed node: " + member );
			try{ cHasher.removeBucket( member ); }
			catch( InterruptedException e ){}
		}
		else {
		    if(!cHasher.containsBucket( member )) {
		        LOGGER.info( "Added node: " + member );
	            cHasher.addBucket( member, vNodes );
	            if(fMgr != null)
	                fMgr.getDatabase().checkHintedHandoffMember( member.getHost(), state );
		    }
		}
		
		// Check whether the number of virtual nodes has been changed.
		int vNodes = gManager.getVirtualNodes();
        if(vNodes != this.vNodes) {
            // If the number of virtual nodes has been changed
            // the previous ouselves member is removed and replaced with
            // the updated value.
            try{ cHasher.removeBucket( gManager.getMyself() ); }
            catch( InterruptedException e ){}
            cHasher.addBucket( gManager.getMyself(), this.vNodes = vNodes );
        }
	}

	/** 
	 * Sets the initial configuration.
	*/
	private void setConfigure() throws IOException
	{
	    if(!DFSUtils.initConfig) {
	        PropertyConfigurator.configure( ResourceLoader.getResourceAsStream( DFSUtils.LOG_CONFIG ) );
		    DFSUtils.initConfig = true;
			BasicConfigurator.configure();
		}
		
		JSONObject file = DFSUtils.parseJSONFile( DISTRIBUTED_FS_CONFIG );
		JSONArray inetwork = file.getJSONArray( "network_interface" );
        String inet = inetwork.getJSONObject( 0 ).getString( "type" );
        int IPversion = inetwork.getJSONObject( 1 ).getInt( "IPversion" );
		
		// Load the address only if it's null.
		if(_address == null)
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
			LOGGER.debug( "Net: " + netInt.getName() );
			if(netInt.getName().equals( inet )) {
				// enumerate all the IP address associated with it
				for(InetAddress inetAddress : Collections.list( netInt.getInetAddresses() )) {
					LOGGER.debug( "Address: " + inetAddress );
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
									DISTRIBUTED_FS_CONFIG );
		
		LOGGER.info( "Address: " + _address );
		
		return _address;
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	*/
	public DistributedFile getFile( final String fileName ) throws InterruptedException
	{
		if(fMgr == null)
			return null;
		
		DistributedFile file = fMgr.getDatabase().getFile( fileName );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Returns the successor nodes of the input id.
	 * 
	 * @param id				source node identifier
	 * @param addressToRemove	the address to skip during the procedure.
	 *                          The address must be in the form {@code hostName:port}
	 * @param numNodes			number of requested nodes
	 * 
	 * @return the list of successor nodes;
	 * 		   it could contains less than {@code numNodes} elements.
	*/
	public List<GossipMember> getSuccessorNodes( final String id, final String addressToRemove, final int numNodes )
	{
		List<GossipMember> nodes = new ArrayList<>( numNodes );
		Set<String> filterAddress = new HashSet<>();
		int size = 0;
		
		filterAddress.add( addressToRemove );
		
		// Choose the nodes whose address is different than this node.
		String currId = id, succ;
		while(size < numNodes) {
			succ = cHasher.getNextBucket( currId );
			if(succ == null || succ.equals( id ))
				break;
			
			GossipMember node = cHasher.getBucket( succ );
			if(node != null) {
				currId = succ;
				if(!filterAddress.contains( node.getAddress() )) {
					nodes.add( node );
					filterAddress.add( node.getAddress() );
					size++;
				}
			}
		}
		
		return nodes;
	}
	
	public ThreadState getJobState() {
		return state;
	}
	
	public boolean isCompleted()
	{
		return completed;
	}
    
    @Override
    public long getId() {
        return id;
    }
    
    public void setId( final long id ) {
    	this.id = id;
    }
    
    protected long getNextThreadID() {
        return nextThreadID = (nextThreadID + 1) % Long.MAX_VALUE;
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
	public void close()
	{
	    if(shutDown.get())
            return;
	    
		shutDown.set( true );
		
		monitor_t.close();
		netMonitor.shutDown();
		
		try{
            netMonitor.join();
            monitor_t.join();
        }
        catch( InterruptedException e ) {}
		
		_net.close();
		if(gManager.isStarted())
		    gManager.shutdown();
		
		synchronized( threadPool ) {
		    threadPool.shutdown();
		}
		
		try { threadPool.awaitTermination( 1, TimeUnit.SECONDS ); }
		catch( InterruptedException e1 ) { e1.printStackTrace(); }
		
		if(fMgr != null)
			fMgr.shutDown();
		
		try{
		    netMonitor.join();
		    monitor_t.join();
		    if(fMgr != null)
	            fMgr.join();
		}
		catch( InterruptedException e ) {}
	}
}