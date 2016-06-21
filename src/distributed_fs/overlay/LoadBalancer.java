/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.json.JSONException;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.NetworkMonitor;
import distributed_fs.net.NetworkMonitorReceiverThread;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.overlay.manager.QuorumSession;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadState;
import distributed_fs.utils.ArgumentsParser;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipService;
import gossiping.GossipSettings;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public class LoadBalancer extends DFSNode
{
	// ===== used by the private constructor ===== //
	private TCPSession session;
	private String clientAddress;
	// =========================================== //
	
	private List<Thread> threadsList;
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.
	 * 
	 * @param ipAddress			the ip address. If {@code null} it will be taken using the configuration file parameters.
	 * @param port				port used to receive incoming requests.
	 * 							If the value is less or equal than 0,
	 * 							then the default one will be chosed ({@link DFSUtils#SERVICE_PORT});
	 * @param startupMembers	list of nodes
	*/
	public LoadBalancer( final String ipAddress,
						 final int port,
						 final List<GossipMember> startupMembers ) throws IOException, JSONException, InterruptedException
	{
		super( GossipMember.LOAD_BALANCER, ipAddress, startupMembers ); 
		
		if(startupMembers != null) {
			// Start the gossiping from the input list.
			String id = DFSUtils.getNodeId( 1, _address );
			GossipSettings settings = new GossipSettings();
			GossipService gossipService = new GossipService( _address, GossipManager.GOSSIPING_PORT, id, computeVirtualNodes(),
															 GossipMember.LOAD_BALANCER, startupMembers, settings, this );
			gossipService.start();
		}
		
		this.port = (port <= 0) ? DFSUtils.SERVICE_PORT : port;
		
		netMonitor = new NetworkMonitorReceiverThread( _address );
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
		
		launch();
	}
	
	/** Testing. */
	public LoadBalancer( final List<GossipMember> startupMembers,
						 final int port,
						 final String address ) throws IOException, JSONException, SQLException
	{
		super();
		
		_address = address;
		
		for(GossipMember member : startupMembers) {
			if(member.getNodeType() != GossipMember.LOAD_BALANCER)
				gossipEvent( member, GossipState.UP );
		}
		
		netMonitor = new NetworkMonitorReceiverThread( _address );
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
		
		this.port = port;
	}
	
	public void launch() throws JSONException
	{
	    netMonitor.start();
	    
		try {
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//LOGGER.debug( "[LB] Waiting on: " + _address + ":" + this.port );
				TCPSession session = _net.waitForConnection( _address, this.port );
				if(session != null) {
				    synchronized( threadPool ) {
                        if(threadPool.isShutdown())
                            break;
                        
                        LoadBalancer node = new LoadBalancer( _net, session, cHasher, netMonitor );
                        monitor_t.addThread( node );
                        
                        threadPool.execute( node );
                    }
				}
				
				// Check if the monitor thread is alive: if not a new instance is activated.
                if(!monitor_t.isAlive()) {
                    monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
                    monitor_t.start();
                }
			}
		}
		catch( IOException e ) {}
		
		System.out.println( "[LB] Closed." );
	}
	
	/**
	 * Constructor used to handle an incoming request.
	 * 
	 * @param net          the net it is connected to
	 * @param srcSession   the input request
	 * @param cHasher      the consistent hashing
	 * @param netMonitor   the network monitor   
	*/
	private LoadBalancer( final TCPnet net,
						  final TCPSession srcSession,
						  final ConsistentHasherImpl<GossipMember, String> cHasher,
						  final NetworkMonitor netMonitor ) throws JSONException, IOException
	{
		super( net, null, cHasher );
		
		this.netMonitor = netMonitor;
		session = srcSession;
		
		actionsList = new ArrayDeque<>( 16 );// TODO per adesso e' 16 poi si vedra'
        state = new ThreadState( id, actionsList, fMgr, null, cHasher, this.netMonitor );
	}
	
	@Override
	public void run()
	{
		//LOGGER.info( "[LB] Handling an incoming connection..." );
		
		//while(true) {
		TCPSession newSession = null;
		
		try {
		    MessageRequest data = DFSUtils.deserializeObject( session.receiveMessage() );
			// Get the operation type.
			byte opType = data.getType();
			clientAddress = data.getMetadata().getClientAddress();
			
			String fileName = data.getFileName();
			
			LOGGER.debug( "[LB] Received: " + getCodeString( opType ) + ":" + fileName );
			
			// Get the node associated to the file.
			String nodeId = cHasher.getNextBucket( DFSUtils.getId( fileName ) );
			if(nodeId != null) {
				GossipMember node = cHasher.getBucket( nodeId );
				if(node != null) {
					System.out.println( "OWNER: " + node );
					
					// Send the request to the "best" node, based on load informations.
					String hintedHandoff = null;
					List<GossipMember> nodes = getNodesFromPreferenceList( nodeId, node );
					LOGGER.debug( "[LB] Nodes: " + nodes );
					
					for(int i = nodes.size() - 1; i >= 0; i--) {
						GossipMember targetNode = getBalancedNode( nodes );
						
						// Contact the target node.
						LOGGER.debug( "[LB] Contacting: " + targetNode );
						try{ newSession = _net.tryConnect( targetNode.getHost(), targetNode.getPort(), 2000 ); }
						catch( IOException e ){
						    // Ignored.
							//e.printStackTrace();
						}
						
						if(newSession == null) {
							LOGGER.debug( "[LB] Node " + targetNode + " is unreachable." );
							
							if(opType == Message.PUT && hintedHandoff == null)
								hintedHandoff = targetNode.getHost() + ":" + targetNode.getPort();
							LOGGER.debug( "Hinted Handoff: " + hintedHandoff );
							
							nodes.remove( targetNode );
						}
						else {
							// Notify the client that a remote node is available.
							MessageResponse response = new MessageResponse( Message.TRANSACTION_OK );
							session.sendMessage( response, true );
							
							// Send the message to the target node.
							forwardRequest( newSession, opType, targetNode.getId(), hintedHandoff, fileName, data.getPayload() );
							newSession.close();
							LOGGER.info( "[LB] Request forwarded to: " + targetNode );
							break;
						}
					}
					
					if(newSession == null) {
						MessageResponse response = new MessageResponse( Message.TRANSACTION_FAILED );
						session.sendMessage( response, true );
					}
				}
			}
			else {
				LOGGER.info( "There are no available nodes. The transaction will be closed." );
				MessageResponse response = new MessageResponse( Message.TRANSACTION_FAILED );
				session.sendMessage( response, true );
			}
			
			//session.close();
		}
		catch( IOException e ){
			//e.printStackTrace();
			//session.close();
			if(newSession != null)
				newSession.close();
			
			//break;
		}
		//}
		
		session.close();
		LOGGER.info( "[LB] Closed request from: " + clientAddress );
	}
	
	/**
	 * Gets the first N nodes from the node's preference list,
	 * represented by its identifier.<br>
	 * For simplicity, its preference list is made by nodes
	 * encountered while walking the DHT.
	 * 
	 * @param id			the input node identifier
	 * @param sourceNode	the source node that have started the procedure
	 * 
	 * @return list of nodes taken from the given node's preference list.
	*/
	private List<GossipMember> getNodesFromPreferenceList( final String id, final GossipMember sourceNode )
	{
		final int PREFERENCE_LIST = QuorumSession.getMaxNodes();
		List<GossipMember> nodes = getSuccessorNodes( id, sourceNode.getHost(), PREFERENCE_LIST );
		nodes.add( sourceNode );
		return nodes;
	}

	/**
	 * Gets the most balanced node to which send the request.
	 * 
	 * @param nodes		list of nodes
	 * 
	 * @return the most balanced node, if present, {@code null} otherwise
	*/
	private GossipMember getBalancedNode( final List<GossipMember> nodes )
	{
		double minWorkLoad = Double.MAX_VALUE;
		Integer targetNode = null;
		
		for(int i = 0; i < nodes.size(); i++) {
			GossipMember node = nodes.get( i );
			NodeStatistics stats = netMonitor.getStatisticsFor( node.getHost() );
			if(stats != null) {
				double workLoad = stats.getAverageLoad();
				if(workLoad < minWorkLoad) {
					minWorkLoad = workLoad;
					targetNode = i;
				}
			}
		}
		
		return nodes.get( (targetNode == null) ? 0 : targetNode );
	}

	/** 
	 * Forward the message to the target node.
	 * 
	 * @param session
	 * @param opType
	 * @param destId
	 * @param hintedHandoff
	 * @param fileName
	 * @param file
	*/
	private void forwardRequest( final TCPSession session, final byte opType, final String destId,
	                             final String hintedHandoff, final String fileName, final byte[] file ) throws IOException
	{
		MessageRequest message;
		Metadata meta = new Metadata( clientAddress, hintedHandoff );
		
		if(opType == Message.GET_ALL)
			message = new MessageRequest( opType, fileName, null, false, null, meta );
		else {
    		if(opType != Message.GET) // PUT and DELETE operations
    		    message = new MessageRequest( opType, fileName, file, true, destId, meta );
    		else // GET operation
    		    message = new MessageRequest( opType, fileName, null, true, destId, meta );
		}
		
		session.sendMessage( DFSUtils.serializeObject( message ), true );
	}
	
	/**
     * Start a thread, replacing an inactive one.
     * 
     * @param threadPool
     * @param state
    */
    public static DFSNode startThread( final ExecutorService threadPool, final ThreadState state ) throws IOException, JSONException
    {
        LoadBalancer node =
                new LoadBalancer( state.getNet(),
                                  state.getSession(),
                                  state.getHashing(),
                                  state.getNetMonitor() );
        
        synchronized( threadPool ) {
            if(threadPool.isShutdown())
                return null;
            
            threadPool.execute( node );
        }
        
        return node;
    }
	
	public static void main( String args[] ) throws Exception
	{
		ArgumentsParser.parseArgs( args, GossipMember.LOAD_BALANCER );
		
		String ipAddress = ArgumentsParser.getIpAddress();
		int port = ArgumentsParser.getPort();
		List<GossipMember> members = ArgumentsParser.getNodes();
		
		new LoadBalancer( ipAddress, port, members );
	}
}