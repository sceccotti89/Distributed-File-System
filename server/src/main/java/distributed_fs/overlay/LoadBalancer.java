/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.exception.DFSException;
import distributed_fs.net.NetworkMonitorReceiverThread;
import distributed_fs.net.NetworkMonitorThread;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadMonitor.ThreadState;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipNode;

public class LoadBalancer extends DFSNode
{
	// ===== used by the private constructor ===== //
	private TCPSession session;
	private String clientAddress;
	private boolean replacedThread;
	// =========================================== //
	
	private List<Thread> threadsList;
	
	
	
	
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.
	 * 
	 * @param ipAddress			the ip address. If {@code null} it will be taken using the configuration file parameters.
	 * @param port				port used to receive incoming requests. If the value is less or equal than 0,
	 * 							then the default one will be chosed ({@link DFSUtils#SERVICE_PORT});
	 * @param startupMembers	list of nodes
	*/
	public LoadBalancer( final String ipAddress,
						 final int port,
						 final List<GossipMember> startupMembers ) throws IOException, InterruptedException
	{
		super( ipAddress, port, 1, GossipMember.LOAD_BALANCER, startupMembers );
		setName( "StorageNode" );
		
		// Set the id to the remote nodes.
        List<GossipNode> nodes = runner.getGossipService().getGossipManager().getMemberList();
        for(GossipNode node : nodes) {
            GossipMember member = node.getMember();
            member.setId( DFSUtils.getNodeId( 1, member.getAddress() ) );
        }
		
		if(startupMembers != null) {
		    for(GossipMember member : startupMembers) {
	            if(member.getVirtualNodes() > 0 &&
	               member.getNodeType() != GossipMember.LOAD_BALANCER)
	                cHasher.addBucket( member, member.getVirtualNodes() );
	        }
		}
		
		netMonitor = new NetworkMonitorReceiverThread( _address );
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, this.port, MAX_USERS );
	
		this.port += PORT_OFFSET;
	}
	
	/**
     * Load the StorageNode from the configuration file.
     * 
     * @param configFile   the configuration file
    */
    public static LoadBalancer fromJSONFile( final JSONObject configFile ) throws IOException, InterruptedException, DFSException
    {
        String address = configFile.has( "Address" ) ? configFile.getString( "Address" ) : null;
        int port = configFile.has( "Port" ) ? configFile.getInt( "Port" ) : 0;
        List<GossipMember> members = getStartupMembers( configFile );
        
        return new LoadBalancer( address, port, members );
    }
    
    /**
     * Removes a given node from its data structures.
     * 
     * @param member    member to add
    */
    public void removeNode( final GossipMember member ) throws InterruptedException
    {
        cHasher.removeBucket( member );
        runner.getGossipService().getGossipManager().removeMember( member );
    }
    
    /**
     * Adds a given node to its data structures.
     * 
     * @param member    member to remove
    */
    public void addNode( final GossipMember member )
    {
        cHasher.addBucket( member, member.getVirtualNodes() );
        runner.getGossipService().getGossipManager().addMember( member );
    }
	
	/**
     * Starts the node.<br>
     * It can be launched in an asynchronous way, creating a new Thread that
     * runs this process.
     * 
     * @param launchAsynch   {@code true} to launch the process asynchronously,
     *                       {@code false} otherwise
    */
    public void launch( final boolean launchAsynch )
    {
        if(launchAsynch) {
            // Create a new Thread.
            Thread t = new Thread() {
                @Override
                public void run() {
                    startProcess();
                }
            };
            t.setName( "LoadBalancer" );
            t.setDaemon( true );
            t.start();
        }
        else {
            startProcess();
        }
    }
	
	private void startProcess()
	{
	    netMonitor.start();
	    monitor_t.start();
	    if(startGossiping)
	        runner.start();
	    
	    LOGGER.info( "[LB] Waiting on: " + _address + ":" + port );
	    
		try {
			while(!shutDown.get()) {
				TCPSession session = _net.waitForConnection( _address, this.port );
				if(session != null) {
				    synchronized( threadPool ) {
                        if(threadPool.isShutdown())
                            break;
                        
                        LoadBalancer node = new LoadBalancer( false, _net, session, cHasher, netMonitor );
                        monitor_t.addThread( node );
                        threadPool.execute( node );
                    }
				}
				
				// Check if the monitor thread is alive: if not a new instance is activated.
                if(!shutDown.get() && !monitor_t.isAlive()) {
                    monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port, MAX_USERS );
                    monitor_t.start();
                }
			}
		}
		catch( IOException e ) {
		    if(!shutDown.get()) {
		        e.printStackTrace();
		        close();
		    }
		}
		
		LOGGER.info( "[LB] '" + _address + ":" + port + "' Closed." );
	}
	
	/**
	 * Constructor used to handle an incoming request.
	 * 
	 * @param replacedThread   {@code true} if the thread replace an old one, {@code false} otherwise
	 * @param net              the net it is connected to
	 * @param srcSession       the input request
	 * @param cHasher          the consistent hashing
	 * @param netMonitor       the network monitor   
	*/
	private LoadBalancer( final boolean replacedThread,
	                      final TCPnet net,
						  final TCPSession srcSession,
						  final ConsistentHasher<GossipMember, String> cHasher,
						  final NetworkMonitorThread netMonitor ) throws IOException
	{
		super( net, null, cHasher );
		setName( "LoadBalancer" );
		
		this.replacedThread = replacedThread;
		this.netMonitor = netMonitor;
		session = srcSession;
		
		actionsList = new ArrayDeque<>( 8 );
        state = new ThreadState( id, replacedThread, actionsList,
                                 fMgr, null, cHasher, this.netMonitor );
	}
	
	@Override
	public void run()
	{
		try {
		    MessageRequest data = state.getValue( ThreadState.NEW_MSG_REQUEST );
            if(data == null) {
                data = session.receiveMessage();
                state.setValue( ThreadState.NEW_MSG_REQUEST, data );
            }
            
			// Get the operation type.
			byte opType = data.getType();
			clientAddress = data.getMetadata().getClientAddress();
			LOGGER.info( "[LB] New request from: " + clientAddress );
			
			String fileName = (opType == Message.GET_ALL) ? DFSUtils.createRandomFile() :
                                                            data.getFileName();
			
			LOGGER.debug( "[LB] Received: " + getCodeString( opType ) + ":" + fileName );
			
			// Get the unique identifier associated to the file.
			String nodeId = state.getValue( ThreadState.GOSSIP_NODE_ID );
			if(nodeId == null) {
			    nodeId = cHasher.getNextBucket( DFSUtils.getId( fileName ) );
			    state.setValue( ThreadState.GOSSIP_NODE_ID, nodeId );
			}
			// Get the node associated to the file.
			GossipMember node = null;
			if(nodeId != null) {
			    node = state.getValue( ThreadState.GOSSIP_NODE );
	            if(node == null) {
	                node = cHasher.getBucket( nodeId );
	                state.setValue( ThreadState.GOSSIP_NODE, node );
	            }
			}
			
			if(node != null) {
			    LOGGER.debug( "[LB] Owner: " + node );
				
				List<GossipMember> nodes = getNodesFromPreferenceList( nodeId, node );
				LOGGER.debug( "[LB] Nodes: " + nodes );
				
				String hintedHandoff = null;
				TCPSession newSession = null;
				Integer index = state.getValue( ThreadState.NODES_INDEX );
				if(index == null) index = nodes.size();
				for(int i = index - 1; i >= 0; i--) {
				    // Send the request to the "best" node, based on load informations.
					GossipMember targetNode = getBalancedNode( nodes );
					int port = targetNode.getPort();
					
					// Contact the target node.
					LOGGER.debug( "[LB] Contacting: " + targetNode );
					if(!replacedThread || actionsList.isEmpty()) {
    					try{ newSession = _net.tryConnect( targetNode.getHost(), port + PORT_OFFSET, 2000 ); }
    					catch( IOException e ){ /* Ignored. e.printStackTrace();*/ }
    					state.setValue( ThreadState.BALANCED_NODE_CONN, newSession );
    					actionsList.addLast( DONE );
					}
					else {
					    newSession = state.getValue( ThreadState.BALANCED_NODE_CONN );
					    actionsList.removeFirst();
					}
					
					if(newSession != null) {
					    // Notify the client that a remote node is available.
                        sendClientResponse( Message.TRANSACTION_OK );
                        
                        // Send the message to the target node.
                        forwardRequest( newSession, opType, targetNode.getId(), hintedHandoff, fileName, data.getPayload() );
                        newSession.close();
                        LOGGER.info( "[LB] Request forwarded to: " + targetNode );
                        break;
					}
					else {
					    LOGGER.debug( "[LB] Node " + targetNode + " is unreachable." );
					    nodes.remove( targetNode );
                        
                        if(opType == Message.PUT && hintedHandoff == null) {
                            hintedHandoff = targetNode.getHost() + ":" + port;
                            LOGGER.debug( "[LB] Hinted Handoff: " + hintedHandoff );
                        }
					}
					
					state.setValue( ThreadState.NODES_INDEX, i );
				}
				
				if(newSession == null)
				    sendClientResponse( Message.TRANSACTION_FAILED );
			}
			else
			    sendClientResponse( Message.TRANSACTION_FAILED );
		}
		catch( IOException e ){
		    // Ignored.
			//e.printStackTrace();
		}
		
		session.close();
		LOGGER.info( "[LB] Closed request from: " + clientAddress );
	}
	
	/**
	 * Gets the first N nodes from the given node's preference list,
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
		List<GossipMember> nodes = state.getValue( ThreadState.SUCCESSOR_NODES );
		if(nodes == null) {
		    nodes = new ArrayList<>( PREFERENCE_LIST + 1 );
		    nodes.add( sourceNode );
		    nodes.addAll( getSuccessorNodes( id, sourceNode.getAddress(), PREFERENCE_LIST ) );
		    state.setValue( ThreadState.SUCCESSOR_NODES, nodes );
		}
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
	 * @param session          the TCP session opened with the client
     * @param opType           the operation type
     * @param destId           the destination identifier
     * @param hintedHandoff    the hinted handoff address
     * @param fileName         the requested file
     * @param fileContent      content of the file
	*/
	private void forwardRequest( final TCPSession session, final byte opType, final String destId,
	                             final String hintedHandoff, final String fileName, final byte[] fileContent )
	                                     throws IOException
	{
	    if(!replacedThread || actionsList.isEmpty()) {
    		MessageRequest message;
    		Metadata meta = new Metadata( clientAddress, hintedHandoff );
    		
    		if(opType == Message.GET_ALL)
    			message = new MessageRequest( opType, "", null, false, null, meta );
    		else {
        		if(opType != Message.GET) // PUT and DELETE operations
        		    message = new MessageRequest( opType, fileName, fileContent, true, destId, meta );
        		else // GET operation
        		    message = new MessageRequest( opType, fileName, null, true, destId, meta );
    		}
    		
    		session.sendMessage( DFSUtils.serializeObject( message ), true );
    		actionsList.addLast( DONE );
        }
        else
            actionsList.removeFirst();
	}
	
	/**
	 * Sends to the client the response.
	 * 
	 * @param state
	*/
	private void sendClientResponse( final byte state ) throws IOException
	{
	    if(state == Message.TRANSACTION_FAILED)
	        LOGGER.info( "There are no available nodes. The transaction will be closed." );
	    
	    if(!replacedThread || actionsList.isEmpty()) {
            MessageResponse response = new MessageResponse( state );
            session.sendMessage( response, true );
            actionsList.addLast( DONE );
	    }
	    else
	        actionsList.removeFirst();
	}
	
	/**
     * Start a thread, replacing an inactive one.
     * 
     * @param threadPool    the thread pool executor
     * @param state         the thread state used to load the new one
    */
    public static DFSNode startThread( final ExecutorService threadPool, final ThreadState state ) throws IOException
    {
        LoadBalancer node =
        new LoadBalancer( true,
                          state.getNet(),
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
}
