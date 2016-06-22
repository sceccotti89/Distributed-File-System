/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.NetworkMonitor;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadState;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransferThread;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipRunner;
import gossiping.LogLevel;
import gossiping.event.GossipListener;
import gossiping.event.GossipState;

public abstract class DFSNode extends Thread implements GossipListener
{
	protected static String _address;
	protected int port;
	protected NetworkMonitor netMonitor;
	protected static NodeStatistics stats;
	protected ExecutorService threadPool;
	protected ThreadMonitor monitor_t;
	protected TCPnet _net;
	protected FileTransferThread fMgr;
	protected ConsistentHasherImpl<GossipMember, String> cHasher;
	protected HashSet<String> filterAddress;
	protected GossipRunner runner;
	
	protected boolean shutDown = false;
	
	protected ThreadState state;
	// Actions performed by the thread, during the request processing.
	protected Deque<Integer> actionsList;
	
	protected long id;
	protected boolean completed = false; // Used to check if the thread has completed the job.
	private long nextThreadID; // Next unique identifier associated to the Thread.
	
	protected static final int MAX_USERS = 64; // Maximum number of accepted connections.
	public static final int WAIT_CLOSE = 200;
	public static final Logger LOGGER = Logger.getLogger( DFSNode.class.getName() );
	
	public DFSNode( final int nodeType,
					final String address,
					final List<GossipMember> startupMembers ) throws IOException, JSONException
	{
		_address = address;
		
		setConfigure();
		
		if(!DFSUtils.testing)
			LOGGER.setLevel( DFSUtils.logLevel );
		
		cHasher = new ConsistentHasherImpl<>();
		stats = new NodeStatistics();
		_net = new TCPnet();
		
		//threadPool = Executors.newCachedThreadPool();
		threadPool = Executors.newFixedThreadPool( MAX_USERS );
		
		if(startupMembers == null || startupMembers.size() == 0)
			runner = new GossipRunner( new File( DFSUtils.DISTRIBUTED_FS_CONFIG ), this, _address, computeVirtualNodes(), nodeType );
		
		Runtime.getRuntime().addShutdownHook( new Thread( new Runnable() 
		{
			@Override
			public void run() 
			{
				closeResources();
				LOGGER.info( "Service has been shutdown..." );
			}
		}));
	}
	
	/** Testing. */
	public DFSNode() throws IOException, JSONException
	{
		DFSUtils.testing = true;
		if(!DFSUtils.initConfig){
		    DFSUtils.initConfig = true;
            BasicConfigurator.configure();
        }
		
		cHasher = new ConsistentHasherImpl<>();
		stats = new NodeStatistics();
		_net = new TCPnet();
		//threadPool = Executors.newCachedThreadPool();
		threadPool = Executors.newFixedThreadPool( MAX_USERS );
	}
	
	/**
	 * Constructor used to handle an instance of the Distributed node,
	 * due to an incoming request.
	*/
	public DFSNode( final TCPnet net,
					final FileTransferThread fMgr,
					final ConsistentHasherImpl<GossipMember, String> cHasher )
	{
		this._net = net;
		this.fMgr = fMgr;
		this.cHasher = cHasher;
		//DFSUtils.createDirectory( DFSDatabase.RESOURCES_LOCATION );
	}
	
	/**
	 * Returns the number of virtual nodes that you can manage,
	 * based on the capabilities of this machine.
	*/
	protected short computeVirtualNodes() throws IOException
	{
		short virtualNodes = 2;
		
		Runtime runtime = Runtime.getRuntime();
		
		/* Total number of processors or cores available to the JVM */
		int cores = runtime.availableProcessors();
		LOGGER.debug( "Available processors: " + cores + ", CPU nodes: " + (cores * 4) );
		virtualNodes = (short) (virtualNodes + (cores * 4));
		
		/* Size of the RAM */
		long RAMsize;
		String OS = System.getProperty( "os.name" ).toLowerCase();
		if(OS.startsWith( "windows" )) { // windows command
			ProcessBuilder pb = new ProcessBuilder( "wmic", "computersystem", "get", "TotalPhysicalMemory" );
			Process proc = pb.start();
            //Process proc = runtime.exec( "wmic computersystem get TotalPhysicalMemory" );
			short count = 0;
			
			InputStream stream = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader( stream );
			BufferedReader br = new BufferedReader( isr );
			
			String line = null;
			while((line = br.readLine()) != null && ++count < 3);
			
			br.close();
			
			//System.out.println( line );
			RAMsize = Long.parseLong( line.trim() );
		}
		else { // linux command
			ProcessBuilder pb = new ProcessBuilder( "less", "/proc/meminfo" );
			Process proc = pb.start();
			
			InputStream stream = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader( stream );
			BufferedReader br = new BufferedReader( isr );
			
			String line = null;
			while((line = br.readLine()) != null) {
				if(line.startsWith( "MemTotal" ))
					break;
			}
			
			br.close();
			
			Matcher matcher = Pattern.compile( "[0-9]+(.*?)[0-9]" ).matcher( line );
			matcher.find();
			// Multiply it by 1024 because the result is in kB
			RAMsize = Long.parseLong( line.substring( matcher.start(), matcher.end() ) ) * 1024;
		}
		
		LOGGER.debug( "RAM size: " + RAMsize + ", RAM nodes: " + (RAMsize / 262144000) );
		virtualNodes = (short) (virtualNodes + (RAMsize / 262144000)); // divide it by 250MB
		
		LOGGER.debug( "Total nodes: " + virtualNodes );
		
		return virtualNodes;
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
		    // TODO RIMETTERE
			//TODO LOGGER.info( "Added node: " + member.toJSONObject().toString() );
			cHasher.addBucket( member, member.getVirtualNodes() );
			if(fMgr != null) {
				fMgr.getDatabase().checkHintedHandoffMember( member.getHost(), state );
			}
		}
	}

	/** 
	 * Sets the initial configuration.
	*/
	private void setConfigure() throws IOException, JSONException
	{
		if(!DFSUtils.initConfig){
		    DFSUtils.initConfig = true;
			BasicConfigurator.configure();
		}
		
		JSONObject file = DFSUtils.parseJSONFile( DFSUtils.DISTRIBUTED_FS_CONFIG );
		int logLevel = LogLevel.fromString( file.getString( "log_level" ) );
		DFSUtils.logLevel = LogLevel.getLogLevel( logLevel );
		
		JSONArray inetwork = file.getJSONArray( "network_interface" );
		String inet = inetwork.getJSONObject( 0 ).getString( "inet" );
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
									DFSUtils.DISTRIBUTED_FS_CONFIG );
		
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
	 * @param addressToRemove	the address to skip during the procedure
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
		
		if(!DFSUtils.testing)
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
				if(!filterAddress.contains( node.getHost() )) {
					nodes.add( node );
					if(!DFSUtils.testing)
						filterAddress.add( node.getHost() );
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
	public void closeResources()
	{
		shutDown = true;
		
		netMonitor.shutDown();
		_net.close();
		if(runner != null)
			runner.getGossipService().shutdown();
		synchronized( threadPool ) {
		    threadPool.shutdown();
		}
		if(fMgr != null)
			fMgr.shutDown();
		
		try{ netMonitor.join(); }
		catch( InterruptedException e ) {}
	}
}