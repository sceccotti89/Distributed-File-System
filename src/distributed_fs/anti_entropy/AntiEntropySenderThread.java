/**
 * @author Stefano Ceccotti
*/

package distributed_fs.anti_entropy;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import distributed_fs.anti_entropy.MerkleTree.Node;
import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.overlay.manager.QuorumSession;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransferThread;
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
	private BitSet bitSet = new BitSet(); /** Used to keep track of the different nodes */
	private final HashSet<String> addresses = new HashSet<>();
	
	/** Updating timer. */
	public static final int EXCH_TIMER = 5000;
	
	public AntiEntropySenderThread( final GossipMember _me,
									final DFSDatabase _database,
									final FileTransferThread fMgr,
									final ConsistentHasherImpl<GossipMember, String> cHasher ) throws SocketException
	{
		super( _me, _database, fMgr, cHasher );
	}
	
	@Override
	public void run()
	{
		LOGGER.info( "Anti Entropy Sender Thread launched" );
		
		if(!DFSUtils.testing)
			addresses.add( me.getHost() );
		
		while(!shoutDown) {
			try{ Thread.sleep( EXCH_TIMER ); }
			catch( InterruptedException e ){ break; }
			
			// Each virtual node sends the Merkle tree to its successor node,
			// and to a random predecessor node.
			List<ByteBuffer> vNodes = cHasher.getVirtualBucketsFor( me );
			for(ByteBuffer vNodeId : vNodes) {
				//ByteBuffer succId = cHasher.getSuccessor( vNodeId );
				ByteBuffer succId = cHasher.getNextBucket( vNodeId );
				if(succId != null) {
					GossipMember succNode = cHasher.getBucket( succId );
					if(succNode != null) {
						if(!addresses.contains( succNode.getHost() )) {
							if(!DFSUtils.testing)
								addresses.add( succNode.getHost() );
							try{ startAntiEntropy( succNode, vNodeId.array(), vNodeId, MERKLE_FROM_MAIN ); }
							catch( IOException | InterruptedException e ){ /*e.printStackTrace();*/ }
						}
					}
				}
				
				List<ByteBuffer> nodes = getPredecessorNodes( vNodeId, QuorumSession.getMaxNodes() );
				while(nodes.size() > 0) {
					ByteBuffer randomPeer = selectPartner( nodes );
					GossipMember node = cHasher.getBucket( randomPeer );
					if(node != null) {
						try {
							startAntiEntropy( node, vNodeId.array(), randomPeer, MERKLE_FROM_REPLICA );
							break;
						}
						catch( IOException | InterruptedException e ){ /*e.printStackTrace();*/ }
					}
					
					nodes.remove( randomPeer );
				}
			}
		}
		
		LOGGER.info( "Anti-entropy Sender Thread closed." );
	}
	
	/**
	 * Sends the Merkle tree to the given node.
	 * 
	 * @param address		the node that will receive the Merkle Tree
	 * @param sourceId		the actual virtual node identifier
	 * @param destId		node identifier from which calculate the range of the files
	 * @param msg_type		the type of the exchanged messages
	*/
	private void startAntiEntropy( final GossipMember node, final byte[] sourceId, final ByteBuffer destId, final byte msg_type )
	        throws IOException, InterruptedException
	{
		// ================ TODO finiti i test togliere questa parte ================= //
		/*if(me.getPort() == 8426)
			System.out.println( "DESTINATARIO: " + node.getPort() + ", TYPE: " + msg_type );
		if(me.getPort() == 8426 && node.getPort() == 8002)
			System.out.println( "INVIO AL NODO CHE MI INTERESSA!!!!!!!!!!!!!!!!!" );
		else
			return;
		// =========================================================================== */
		
		//ByteBuffer fromId = cHasher.getPredecessor( nodeId );
		//if(fromId == null) fromId = cHasher.getLastKey();
		ByteBuffer fromId = cHasher.getPreviousBucket( destId );
		List<DistributedFile> files = database.getKeysInRange( fromId, destId );
		m_tree = createMerkleTree( files );
		
		LOGGER.debug( "Vnode: " + DFSUtils.bytesToHex( sourceId ) );
		LOGGER.debug( "FILES: " + files );
		LOGGER.debug( "Type: " + msg_type + ", from: " + cHasher.getBucket( fromId ).getPort() + ", to: " + cHasher.getBucket( destId ).getPort() );
		
		//session = Net.tryConnect( address, MERKLE_TREE_EXCHANGE_PORT, 2000 );
		session = Net.tryConnect( node.getHost(), node.getPort() + 2, 2000 );
		
		handShake( msg_type, sourceId, destId );
		// Check the differences among the trees.
		checkTreeDifferences();
		
		if(m_tree != null && bitSet.cardinality() > 0) {
			LOGGER.debug( "ID: " + DFSUtils.bytesToHex( sourceId ) + ", BIT_SET: " + bitSet );
			// Create and send the list of versions.
			session.sendMessage( getVersions( files ), true );
		}
		
		session.close();
	}
	
	/**
	 * Start the handshake phase.
	 * 
	 * @param msg_type		one of {@code MERKLE_FROM_MAIN} and {@code MERKLE_FROM_REPLICA}
	 * @param sourceId		source node identifier
	 * @param destId		destination node identifier
	*/
	private void handShake( final byte msg_type, final byte[] sourceId, final ByteBuffer destId ) throws IOException
	{
		byte[] data = Net.createMessage( null, sourceId, true );
		data = Net.createMessage( data, new byte[]{ msg_type, (m_tree == null) ? (byte) 0x0 : (byte) 0x1 }, false );
		if(m_tree != null)
			data = Net.createMessage( data, DFSUtils.intToByteArray( m_tree.getHeight() ), true );
		if(msg_type == MERKLE_FROM_REPLICA)
			data = Net.createMessage( data, destId.array(), true );
		session.sendMessage( data, true );
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
			int inputHeight = DFSUtils.byteArrayToInt( session.receiveMessage() );
			if(inputHeight == 0)
				return;
			
			List<Node> nodes = new LinkedList<>();
			nodes.add( m_tree.getRoot() );
			
			// Reduce the level of the tree if it is greater.
			if(m_tree.getHeight() > inputHeight)
				reduceTree( m_tree.getHeight() - inputHeight, nodes );
			
			int nNodes;
			for(int levels = Math.min( m_tree.getHeight(), inputHeight ); levels > 0 && (nNodes = nodes.size()) > 0; levels--) {
				sendCurrentLevel( nodes );
				
				// Receive the response set.
				LOGGER.debug( "Waiting the response..." );
				BitSet set = BitSet.valueOf( session.receiveMessage() );
				LOGGER.debug( "Received the response" );
				
				if(set.cardinality() == nNodes) {
					LOGGER.debug( "End procedure: cardinality == nodes" );
					bitSet.set( 0, m_tree.getNumLeaves() );
					break;
				}
				else {
					for(int i = set.nextSetBit( 0 ); i >= 0; i = set.nextSetBit( i+1 )) {
						LOGGER.debug( "Index: " + i );
						if(i == Integer.MAX_VALUE)
							break; // or (i+1) would overflow
						else {
							LinkedList<Node> leaves = m_tree.getLeavesFrom( nodes.get( i ) );
							bitSet.set( leaves.getFirst().position, leaves.getLast().position + 1 );
						}
					}
				}
				
				for(int i = 0; i < nNodes; i++) {
					Node n = nodes.remove( 0 );
					if(set.get( i ) == false){
						// Insert the right and left child of this node.
						if(n.left != null) nodes.add( n.left );
						if(n.right != null) nodes.add( n.right );
					}
				}
					
				LOGGER.debug( "Nodes: " + nodes.size() );
			}
		}
	}
	
	/**
	 * Send the current level to the other peer.
	 * 
	 * @param nodes		current nodes in the tree
	*/
	private void sendCurrentLevel( final List<Node> nodes ) throws IOException
	{
		int nNodes = nodes.size();
		
		// If the leaves level is reached we stop to send the current level.
		int maxSize = Integer.BYTES + (Integer.BYTES + MerkleTree.sigLength) * nNodes;
		ByteBuffer buffer = ByteBuffer.allocate( Byte.BYTES + maxSize );
		
		// Put the number of nodes.
		buffer.putInt( nNodes );
		// Put the length and signature of each node.
		for(Node node : nodes)
			buffer.put( node.sig );
		
		LOGGER.debug( "Sending the current level.." );
		session.sendMessage( buffer.array(), true );
	}
	
	/**
	 * Gets the list of versions associated to each equal files (the bit is set to 1).
	 * 
	 * @param files		list of files
	 * 
	 * @return the version represented as a byte array
	*/
	private byte[] getVersions( final List<DistributedFile> files )
	{
		int fileSize = files.size();
		byte[] msg = null;
		
		for(int i = bitSet.nextSetBit( 0 ); i >= 0 && i < fileSize; i = bitSet.nextSetBit( i+1 )) {
			if(i == Integer.MAX_VALUE)
				break; // or (i+1) would overflow
			
			DistributedFile file = files.get( i );
			byte[] vClock = DFSUtils.serializeObject( file.getVersion() );
			msg = Net.createMessage( msg, vClock, true );
		}
		
		return msg;
	}
	
	/**
	 * Returns the predecessor nodes respect to the input id.
	 * 
	 * @param id			source node identifier
	 * @param numNodes		maximum number of nodes required
	 * 
	 * @return the list of predecessor nodes. It could contains less than num_nodes elements.
	*/
	private List<ByteBuffer> getPredecessorNodes( final ByteBuffer id, final int numNodes )
	{
		List<ByteBuffer> predecessors = new ArrayList<>( numNodes );
		int size = 0;
		
		if(!DFSUtils.testing)
			addresses.add( me.getHost() );
		
		// choose the nodes whose address is different than this node
		ByteBuffer currId = id, prev;
		while(size < numNodes) {
			prev = cHasher.getPreviousBucket( currId );
			if(prev == null || prev.equals( id ))
				break;
			
			GossipMember node = cHasher.getBucket( prev );
			if(node != null) {
				currId = prev;
				if(!addresses.contains( node.getHost() )) {
					predecessors.add( currId = prev );
					if(!DFSUtils.testing)
						addresses.add( node.getHost() );
					size++;
				}
			}
		}
		
		/*currId = cHasher.getLastKey();
		if(currId != null) {
			while(size < numNodes) {
				currId = cHasher.getPredecessor( currId );
				if(currId == null || currId.equals( id ))
					break;
				
				GossipMember node = cHasher.getBucket( currId );
				if(node != null && !addresses.contains( node.getHost() )) {
					predecessors.add( currId );
					addresses.add( node.getHost() );
					size++;
				}
			}
		}*/
		
		return predecessors;
	}
}