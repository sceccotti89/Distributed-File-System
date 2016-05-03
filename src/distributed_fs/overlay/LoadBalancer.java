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
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public class LoadBalancer extends DFSnode
{
	private int port;
	
	// ===== used by the private constructor ===== //
	private TCPSession session;
	private String clientAddress;
	// =========================================== //
	
	// TODO fare un costruttore in cui si specificano i nodi da contattare?
	public LoadBalancer() throws IOException, JSONException, SQLException
	{
		super( GossipMember.LOAD_BALANCER );
		
		monitor = new NetworkMonitorReceiverThread( _address );
		monitor.start();
		
		this.port = Utils.SERVICE_PORT;
		_net.setSoTimeout( 2000 );
		while(!GossipManager.doShutdown) {
			TCPSession session = _net.waitForConnection( _address, this.port );
			if(session != null)
				threadPool.execute( new LoadBalancer( _net, session, cHasher ) );
		}
		
		closeResources();
	}
	
	/** Testing. */
	public LoadBalancer( final List<GossipMember> startupMembers,
						 final int port,
						 final String address ) throws IOException, JSONException, SQLException
	{
		super();
		
		_address = address;
		
		for(GossipMember member: startupMembers)
			gossipEvent( member, GossipState.UP );
		
		monitor = new NetworkMonitorReceiverThread( _address );
		monitor.start();
		
		this.port = port;
	}
	
	@Test
	public void launch() throws IOException, JSONException, SQLException
	{
		_net.setSoTimeout( 2000 );
		while(!GossipManager.doShutdown) {
			TCPSession session = _net.waitForConnection( _address, this.port );
			if(session != null)
				threadPool.execute( new LoadBalancer( _net, session, cHasher ) );
		}
		
		System.out.println( "[LB] Closed." );
		
		closeResources();
	}
	
	/**
	 * Constructor used to handle the incoming request.
	 * 
	 * @param net			the net it is connected to
	 * @param srcSession	the input request
	 * @param cHasher		
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
		LOGGER.info( "[LB] Received a new connection from: " + clientAddress );
		
		TCPSession newSession;
		while(true) {
			newSession = null;
			
			try {
				MessageRequest data = Utils.deserializeObject( session.receiveMessage() );
				LOGGER.info( "[LB] Received a new request" );
				
				// get the operation type
				byte opType = data.getType();
				String fileName;
				if(opType == Message.GET_ALL)
					fileName = Utils.createRandomFile();
				else
					fileName = data.getFileName();
				
				LOGGER.debug( "[LB] Received: " + opType + ":" + fileName );
				
				// get the node associated to the file
				ByteBuffer nodeId = cHasher.getSuccessor( Utils.getId( fileName ) );
				if(nodeId == null) nodeId = cHasher.getFirstKey();
				System.out.println( "NODE: " + nodeId );
				if(nodeId != null) {
					GossipMember node = cHasher.getBucket( nodeId );
					if(node != null) {
						// send the request to the "best" node, based on load informations
						String hintedHandoff = null;
						List<GossipMember> nodes = getNodesFromPreferenceList( nodeId, node );
						LOGGER.debug( "[LB] Nodes: " + nodes );
						
						for(int i = 0; i < nodes.size(); i++) {
							GossipMember targetNode = getBalancedNode( nodes );
							
							// contact the target node
							LOGGER.debug( "[LB] Contacting: " + targetNode );
							try{ newSession = _net.tryConnect( targetNode.getHost(), targetNode.getPort(), 2000 ); }
							catch( IOException e ){
								//e.printStackTrace();
							}
							
							if(newSession == null) {
								if(opType == Message.PUT && hintedHandoff == null)
									hintedHandoff = targetNode.getHost() + ":" + (targetNode.getPort() + 1);
							}
							else {
								// notify the client that a remote node is available
								MessageResponse response = new MessageResponse( Message.TRANSACTION_OK );
								session.sendMessage( response, true );
								
								// forward the message to the target node
								forwardRequest( newSession, opType, targetNode.getId(), hintedHandoff, fileName, data.getFile() );
								newSession.close();
								LOGGER.info( "[LB] Request forwarded to: " + targetNode.getHost() );
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
					LOGGER.info( "There is no available nodes" );
					MessageResponse response = new MessageResponse( Message.TRANSACTION_FAILED );
					session.sendMessage( response, true );
				}
			}
			catch( IOException e ){
				//e.printStackTrace();
				if(newSession != null)
					newSession.close();
				
				break;
			}
		}
		
		session.close();
		LOGGER.info( "Closed request from: " + clientAddress );
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
	private List<GossipMember> getNodesFromPreferenceList( final ByteBuffer id, final GossipMember sourceNode )
	{
		final int PREFERENCE_LIST = QuorumSystem.getMaxNodes();
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

	private void forwardRequest( final TCPSession session, final byte opType, final String destId,
								 final String hintedHandoff, final String fileName, final byte[] file ) throws IOException
	{
		MessageRequest message;
		
		if(opType == Message.GET_ALL) {
			message = new MessageRequest( opType, null, null, false, null, clientAddress, null );
		}
		else {
			if(opType != Message.GET) {
				// PUT and DELETE operations
				message = new MessageRequest( opType, null, file, true, destId, clientAddress, hintedHandoff );
			}
			else {
				// GET operation
				message = new MessageRequest( opType, fileName, null, true, destId, clientAddress, null );
			}
		}
		
		session.sendMessage( Utils.serializeObject( message ), true );
	}
	
	public static void main( String args[] ) throws Exception
	{
		new LoadBalancer();
	}
}