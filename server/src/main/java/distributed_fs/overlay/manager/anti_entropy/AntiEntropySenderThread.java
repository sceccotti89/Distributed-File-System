/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager.anti_entropy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.overlay.manager.anti_entropy.MerkleTree.Node;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransfer;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

/**
 * Class used to manage the main database.
 * It periodically checks for updates
 * and sends the current Merkle tree to its neighbours.
 * The BitSet object manage the divergent nodes, 
 * checking if there is any bit equals to 0;
 * if so the exchange procedure is repeated until the leaves are reached.
*/
public class AntiEntropySenderThread extends AntiEntropyThread
{
	private TCPSession session;
	private MerkleTree m_tree = null;
	
	// Used to keep track of the common files.
	private BitSet bitSet = new BitSet();
	private final Set<String> addresses = new HashSet<>();
	
	
	
	
	
	public AntiEntropySenderThread( final GossipMember _me,
									final DFSDatabase _database,
									final FileTransfer fMgr,
									final ConsistentHasher<GossipMember, String> cHasher )
	{
		super( _me, _database, fMgr, cHasher );
		setName( "AntiEntropySender" );
	}
	
	@Override
	public void run()
	{
		LOGGER.info( "Anti Entropy Sender Thread launched" );
		
		// Used to ensure that a request doesn't start too soon.
		try{ Thread.sleep( 300 ); }
        catch( InterruptedException e ){
            LOGGER.info( "Anti-entropy Sender Thread closed." );
            return;
        }
		
		while(!shutDown.get()) {
		    // Each virtual node sends the Merkle tree to a
		    // random successor and predecessor node.
		    List<String> vNodes = cHasher.getVirtualBucketsFor( me );
			for(String vNodeId : vNodes) {
			    // Upper-bound on the number of nodes: it takes all of them.
			    List<String> nodes = getSuccessorNodes( vNodeId, Integer.MAX_VALUE );
                while(nodes.size() > 0) {
                    if(shutDown.get())
                        break;
                    
                    // A random successor node.
                    int randomNodeIndex = selectPartner( nodes );
                    String randomPeer = nodes.get( randomNodeIndex );
                    GossipMember succNode = cHasher.getBucket( randomPeer );
                    if(succNode != null) {
                        try {
                            if(startAntiEntropy( succNode, vNodeId ))
                                break;
                        } catch( IOException e ) {
                            // Ignored.
                            //e.printStackTrace();
                            //System.err.println( "Node: " + me + ", Contacting: " + node );
                            /*System.err.println( "vNodeId: " + cHasher.getBucket( vNodeId ) +
                                                ", From: " + cHasher.getBucket( cHasher.getPreviousBucket( randomPeer ) ) +
                                                ", To: " + cHasher.getBucket( randomPeer ) +
                                                ", Contacting: " + node );*/
                        }
                    }
                    
                    nodes.remove( randomNodeIndex );
                }
			}
			
			try{ TimeUnit.SECONDS.sleep( WAIT_TIMER ); }
            catch( InterruptedException e ){ break; }
		}
		
		LOGGER.info( "Anti-entropy Sender Thread closed." );
	}
	
	/**
	 * Sends the Merkle tree to the given node.
	 * 
	 * @param node          the node that will receive the Merkle Tree
	 * @param vNodeId		the actual virtual node identifier from which calculate the range of the files
	 * 
	 * @return {@code true} if the mechanism has been completed successfully,
	 *         {@code false} otherwise
	*/
	private boolean startAntiEntropy( final GossipMember node,
	                                  final String vNodeId ) throws IOException
	{
	    String prevId = cHasher.getPreviousBucket( vNodeId );
	    List<DistributedFile> files = database.getKeysInRange( prevId, vNodeId );
        m_tree = createMerkleTree( files );
	    
		boolean complete = handShake( node, vNodeId, prevId );
		if(complete) {
    		LOGGER.debug( "vNodeId: " + vNodeId );
    		LOGGER.debug( "Destination node: " + node.getAddress() +
    		              ", fromDHT: " + prevId + ", toDHT: " + vNodeId +
    		              ", FILES: " + files +
    		              ", TREE: " + m_tree );
    		
    		// Check the differences among the trees.
    		checkTreeDifferences();
    		
    		if(m_tree != null && bitSet.cardinality() > 0) {
    			LOGGER.debug( "Id: " + vNodeId + ", BitSet: " + bitSet );
    			sendVersions( files );
    		}
		}
		
		session.close();
		
		return complete;
	}
	
	/**
	 * Starts the handshake phase.
	 * During this phase some important informations
     * are exchanged.
	 * 
	 * @param node         the node that will receive the Merkle Tree
	 * @param sourceId     source node identifier
	 * @param prevId       previous node identifier met in the DHT
	 * 
	 * @return {@code true} if the connection has been completed successfully,
	 *         {@code false} otherwise
	*/
	private boolean handShake( final GossipMember node, final String sourceId, final String prevId ) throws IOException
	{
	    session = net.tryConnect( node.getHost(), node.getPort() + PORT_OFFSET, 2000 );
	    
	    // Add the source id.
		byte[] data = net.createMessage( null, sourceId.getBytes( StandardCharsets.UTF_8 ), true );
		// Add the previous id.
		data = net.createMessage( data, prevId.getBytes( StandardCharsets.UTF_8 ), true );
		// Add the source port number.
		data = net.createMessage( data, DFSUtils.intToByteArray( me.getPort() ), true );
		// Add the own tree status and level.
		data = net.createMessage( data, new byte[]{ (m_tree == null) ? (byte) 0x0 : (byte) 0x1 }, false );
		if(m_tree != null) // If the tree exists also its height is sent.
			data = net.createMessage( data, DFSUtils.intToByteArray( m_tree.getHeight() ), true );
		session.sendMessage( data, true );
		
		// Wait the answer.
		byte[] response = session.receive();
		return (response[0] != Networking.FALSE[0]);
	}
	
	/**
	 * Checks the differences,
	 * sending the tree to the destination node.
	*/
	private void checkTreeDifferences() throws IOException
	{
		bitSet.clear();
		
		if(m_tree != null) {
			// Receive the height of the receiver tree.
			int inputHeight = DFSUtils.byteArrayToInt( session.receive() );
			if(inputHeight == 0)
				return;
			
			Deque<Node> nodes = new ArrayDeque<>( m_tree.getNumNodes() );
			nodes.add( m_tree.getRoot() );
			
			// Reduce the level of the tree if it is greater.
			if(m_tree.getHeight() > inputHeight)
				reduceTree( m_tree.getHeight() - inputHeight, nodes );
			
			int nNodes;
			int levels = Math.min( m_tree.getHeight(), inputHeight );
			boolean finish = false;
			// If the leaves level is reached we stop to send the current level.
			for(; !finish && levels >= 0 && (nNodes = nodes.size()) > 0; levels--) {
				sendCurrentLevel( nodes );
				
				// Receive the response set.
				LOGGER.debug( "Waiting the response..." );
				byte[] msg = session.receive();
				finish = (msg[0] == (byte) 0x1);
				BitSet set = BitSet.valueOf( Arrays.copyOfRange( msg, 1, msg.length ) );
				LOGGER.debug( "Received the response." );
				
				if(set.cardinality() == nNodes) {
					LOGGER.debug( "End procedure: cardinality == nodes." );
					bitSet.set( 0, m_tree.getNumLeaves() );
					break;
				}
				else {
				    int i = -1, index = set.nextSetBit( 0 );
				    if(index >= 0) {
    				    for(Node n : nodes) {
    				        if(++i == index) {
        						LOGGER.debug( "Index: " + index );
        						Deque<Node> leaves = m_tree.getLeavesFrom( n );
    							bitSet.set( leaves.getFirst().position, leaves.getLast().position + 1 );
        						
    							if(index == Integer.MAX_VALUE || (index = set.nextSetBit( index + 1 )) == -1)
                                    break;
    				        }
    					}
				    }
				}
				
				for(int i = 0; i < nNodes; i++) {
					Node n = nodes.removeFirst();
					if(set.get( i ) == false){
						// Insert the right and left child of this node.
						if(n.left != null) nodes.addLast( n.left );
						if(n.right != null) nodes.addLast( n.right );
					}
				}
					
				LOGGER.debug( "Nodes: " + nodes.size() );
			}
		}
	}
	
	/**
	 * Sends the current level to the other peer.
	 * 
	 * @param nodes		current nodes in the tree
	*/
	private void sendCurrentLevel( final Deque<Node> nodes ) throws IOException
	{
		int nNodes = nodes.size();
		int maxSize = Integer.BYTES + MerkleTree.sigLength * nNodes;
		ByteBuffer buffer = ByteBuffer.allocate( maxSize );
		
		// Put the number of nodes.
		buffer.putInt( nNodes );
		// Put the signature of each node.
		for(Node node : nodes)
			buffer.put( node.sig );
		
		LOGGER.debug( "Sending the current level.." );
		session.sendMessage( buffer.array(), true );
	}
	
	/**
	 * Sends the list of versions associated to each common file (the bit is set to 1).
	 * 
	 * @param files		list of files
	*/
	private void sendVersions( final List<DistributedFile> files ) throws IOException
	{
		int fileSize = files.size();
		byte[] msg = null;
		
		for(int i = bitSet.nextSetBit( 0 ); i >= 0 && i < fileSize; i = bitSet.nextSetBit( i+1 )) {
			if(i == Integer.MAX_VALUE)
				break; // or (i+1) would overflow
			
			DistributedFile file = files.get( i );
			byte[] vClock = DFSUtils.serializeObject( file.getVersion() );
			msg = net.createMessage( msg, vClock, true );
		}
		
		session.sendMessage( msg, true );
	}
	
	/**
     * Returns the successor nodes respect to the input id.
     * 
     * @param id            source node identifier
     * @param numNodes      maximum number of nodes required
     * 
     * @return the list of successor nodes. It could contains less than num_nodes elements.
    */
    private List<String> getSuccessorNodes( final String id, final int numNodes )
    {
        List<String> successors = new ArrayList<>( 16 );
        int size = 0;
        
        addresses.clear();
        addresses.add( me.getAddress() );
        
        // Choose the nodes whose address is different than this node.
        String currId = id, prev;
        while(size < numNodes) {
            prev = cHasher.getNextBucket( currId );
            if(prev == null || prev.equals( id ))
                break;
            
            GossipMember node = cHasher.getBucket( prev );
            if(node != null) {
                currId = prev;
                if(!addresses.contains( node.getAddress() )) {
                    successors.add( currId = prev );
                    addresses.add( node.getAddress() );
                    size++;
                }
            }
        }
        
        return successors;
    }
	
	@Override
	public void close()
	{
	    super.close();
	    
	    if(session != null)
	        session.close();
	}
}