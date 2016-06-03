/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;

import org.json.JSONException;
import org.junit.Test;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.NetworkMonitorReceiverThread;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.utils.CmdLineParser;
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
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
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.
	 * 
	 * @param ipAddress			the ip address. If {@code null} it will be taken using the configuration file parameters.
	 * @param port				port used to receive incoming requests.
	 * 							If the value is less or equal than 0,
	 * 							then the default one will be chosed ({@link Utils#SERVICE_PORT});
	 * @param startupMembers	list of nodes
	*/
	public LoadBalancer( final String ipAddress,
						 final int port,
						 final List<GossipMember> startupMembers ) throws IOException, JSONException, InterruptedException
	{
		super( GossipMember.LOAD_BALANCER, ipAddress, startupMembers ); 
		
		if(startupMembers != null) {
			// Start the gossiping from the input list.
			String id = Utils.bytesToHex( Utils.getNodeId( 1, _address ).array() );
			GossipSettings settings = new GossipSettings();
			GossipService gossipService = new GossipService( _address, GossipManager.GOSSIPING_PORT, id, computeVirtualNodes(),
															 GossipMember.LOAD_BALANCER, startupMembers, settings, this );
			gossipService.start();
		}
		
		monitor = new NetworkMonitorReceiverThread( _address );
		monitor.start();
		
		this.port = (port <= 0) ? Utils.SERVICE_PORT : port;
		
		try {
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//System.out.println( "[LB] Waiting on: " + _address + ":" + this.port );
				TCPSession session = _net.waitForConnection( _address, this.port );
				if(session != null)
					threadPool.execute( new LoadBalancer( _net, session, cHasher ) );
			}
		}
		catch( IOException e ){}
		
		//closeResources();
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
		
		monitor = new NetworkMonitorReceiverThread( _address );
		monitor.start();
		
		this.port = port;
	}
	
	@Test
	public void launch() throws JSONException
	{
		try {
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//LOGGER.debug( "[LB] Waiting on: " + _address + ":" + this.port );
				TCPSession session = _net.waitForConnection( _address, this.port );
				if(session != null)
					threadPool.execute( new LoadBalancer( _net, session, cHasher ) );
			}
		}
		catch( IOException e ) {}
		
		System.out.println( "[LB] Closed." );
		
		//closeResources();
	}
	
	/**
	 * Constructor used to handle the incoming request.
	 * 
	 * @param net			the net it is connected to
	 * @param srcSession	the input request
	 * @param cHasher		the consistent hashing
	*/
	private LoadBalancer( final TCPnet net,
						  final TCPSession srcSession,
						  final ConsistentHasherImpl<GossipMember, String> cHasher ) throws JSONException, IOException
	{
		super( net, null, cHasher );
		
		session = srcSession;
		clientAddress = session.getSrcAddress();
	}
	
	@Override
	public void run()
	{
		//LOGGER.info( "[LB] Handling an incoming connection..." );
		
		while(true) {
			TCPSession newSession = null;
			
			try {
				//ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
				MessageRequest data = Utils.deserializeObject( session.receiveMessage() );
				// get the operation type
				byte opType = data.getType();
				
				if(opType == Message.HELLO) {
					clientAddress = data.getMetadata().getClientAddress();
					LOGGER.info( "[LB] Received a new connection from: " + clientAddress );
					continue;
				}
				
				/*byte opType = data.get();
				// get the file name
				String fileName = new String( Utils.getNextBytes( data ) );*/
				
				if(opType == Message.CLOSE) {
					LOGGER.info( "[LB] Received CLOSE request." );
					break;
				}
				
				String fileName;
				if(opType == Message.GET_ALL)
					fileName = Utils.createRandomFile();
				else
					fileName = data.getFileName();
				
				LOGGER.debug( "[LB] Received: " + getCodeString( opType ) + ":" + fileName );
				
				// get the node associated to the file
				/*ByteBuffer nodeId = cHasher.getSuccessor( Utils.getId( fileName ) );
				if(nodeId == null)
					nodeId = cHasher.getFirstKey();*/
				ByteBuffer nodeId = cHasher.getNextBucket( Utils.getId( fileName ) );
				if(nodeId != null) {
					GossipMember node = cHasher.getBucket( nodeId );
					if(node != null) {
						System.out.println( "OWNER: " + node );
						
						// send the request to the "best" node, based on load informations
						String hintedHandoff = null;
						List<GossipMember> nodes = getNodesFromPreferenceList( nodeId, node );
						LOGGER.debug( "[LB] Nodes: " + nodes );
						
						for(int i = nodes.size() - 1; i >= 0; i--) {
							GossipMember targetNode = getBalancedNode( nodes );
							
							// contact the target node
							//try{ newSession = _net.tryConnect( targetNode.getHost(), Utils.SERVICE_PORT, 2000 ); }
							LOGGER.debug( "[LB] Contacting: " + targetNode );
							try{ newSession = _net.tryConnect( targetNode.getHost(), targetNode.getPort(), 2000 ); }
							catch( IOException e ){
								//e.printStackTrace();
							}
							
							if(newSession == null) {
								LOGGER.debug( "[LB] Node " + targetNode + " is unreachable." );
								
								if(opType == Message.PUT && hintedHandoff == null)
									//hintedHandoff = targetNode.getHost();
									hintedHandoff = targetNode.getHost() + ":" + targetNode.getPort();
								
								LOGGER.debug( "Hinted Handoff: " + hintedHandoff );
								
								nodes.remove( targetNode );
							}
							else {
								// notify the client that a remote node is available
								MessageResponse response = new MessageResponse( Message.TRANSACTION_OK );
								session.sendMessage( response, true );
								//session.close();
								//session = newSession;
								
								// forward the message to the target node
								//forwardRequest( opType, targetNode.getId(), hintedHandoff, fileName, data );
								forwardRequest( newSession, opType, targetNode.getId(), hintedHandoff, fileName, data.getData() );
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
					LOGGER.info( "There is no available nodes. The transaction will be closed." );
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
				
				break;
			}
		}
		
		session.close();
		LOGGER.info( "Closed request from: " + clientAddress );
	}
	
	/**
	 * Gets the first N-1 nodes from the node's preference list,
	 * represented by its identifier.<br>
	 * For simplicity, its preference list is made by nodes
	 * encountered while walking the DHT.
	 * 
	 * @param id			the input node identifier
	 * @param sourceNode	the source node that have started the procedure
	 * 
	 * @return list of nodes taken from the given node's preference list.
	*/
	private List<GossipMember> getNodesFromPreferenceList( final ByteBuffer id, final GossipMember sourceNode )
	{
		final int PREFERENCE_LIST = QuorumSystem.getMaxNodes() - 1;
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
			NodeStatistics stats = monitor.getStatisticsFor( node.getHost() );
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
	 * @param opType
	 * @param destId
	 * @param hintedHandoff
	 * @param fileName
	 * @param data
	*/
	/*private void forwardRequest( final byte opType, final String destId, final String hintedHandoff,
								 final String fileName, final ByteBuffer data ) throws IOException
	{
		byte[] message;
		byte startQuorum;
		
		if(opType == Utils.GET_ALL) {
			startQuorum = (byte) 0x0;
			// serialization format: [opType startQuorum clientAddress]
			message = _net.createMessage( new byte[]{ opType, startQuorum }, clientAddress.getBytes( StandardCharsets.UTF_8 ), true );
		}
		else {
			startQuorum = (byte) 0x1;
			if(opType != Utils.GET) {
				// PUT and DELETE operations
				// serialization format: [opType startQuorum clientAddress destId file [hintedHandoff]]
				int length = Byte.BYTES * 2 + Integer.BYTES * 2 + clientAddress.length() + destId.length() + data.remaining();
				ByteBuffer buffer = ByteBuffer.allocate( length );
				byte[] serialFile = Utils.getNextBytes( data );
				//LOGGER.info( "File size: " + serialFile.length );
				buffer.put( opType ).put( startQuorum );
				buffer.putInt( clientAddress.length() ).put( clientAddress.getBytes( StandardCharsets.UTF_8 ) );
				buffer.putInt( destId.length() ).put( destId.getBytes( StandardCharsets.UTF_8 ) );
				buffer.putInt( serialFile.length ).put( serialFile );
				
				message = buffer.array();
				
				if(hintedHandoff != null) {
					// add the hinted handoff address to the end of the message
					message = _net.createMessage( message, hintedHandoff.getBytes( StandardCharsets.UTF_8 ), true );
				}
			}
			else {
				// GET operation
				// serialization format: [opType startQuorum clientAddress destId fileName]
				int length = Byte.BYTES * 2 + Integer.BYTES * 3 + clientAddress.length() + destId.length() + fileName.length();
				ByteBuffer buffer = ByteBuffer.allocate( length );
				buffer.put( opType ).put( startQuorum );
				buffer.putInt( clientAddress.length() ).put( clientAddress.getBytes( StandardCharsets.UTF_8 ) );
				buffer.putInt( destId.length() ).put( destId.getBytes( StandardCharsets.UTF_8 ) );
				buffer.putInt( fileName.length() ).put( fileName.getBytes( StandardCharsets.UTF_8 ) );
				message = buffer.array();
			}
		}
		
		session.sendMessage( message, true );
	}*/
	
	private void forwardRequest( final TCPSession session, final byte opType, final String destId,
	                             final String hintedHandoff, final String fileName, final byte[] file ) throws IOException
	{
		MessageRequest message;
		
		/*if(opType == Message.GET_ALL) {
			message = new MessageRequest( opType, null, null, false, null, clientAddress, null );
		}
		else {*/
		Metadata meta = new Metadata( clientAddress, hintedHandoff );
		if(opType != Message.GET) {
			// PUT and DELETE operations
		    message = new MessageRequest( opType, fileName, file, true, destId, meta );
		}
		else {
			// GET operation
		    message = new MessageRequest( opType, fileName, null, true, destId, meta );
		}
		//}
		
		session.sendMessage( Utils.serializeObject( message ), true );
	}
	
	public static void main( String args[] ) throws Exception
	{
		CmdLineParser.parseArgs( args, GossipMember.LOAD_BALANCER );
		
		String ipAddress = CmdLineParser.getIpAddress();
		int port = CmdLineParser.getPort();
		List<GossipMember> members = CmdLineParser.getNodes();
		
		new LoadBalancer( ipAddress, port, members );
	}
}