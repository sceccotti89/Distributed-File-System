/**
 * @author Stefano Ceccotti
*/

package distributed_fs.merkle_tree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.files.DFSDatabase;
import distributed_fs.files.DistributedFile;
import distributed_fs.files.FileManagerThread;
import distributed_fs.merkle_tree.MerkleTree.Node;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.messages.Message;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.VectorClockInconsistencyResolver;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;

/**
 * Class used to manage the Merkle tree of the replica database.
 * It waits for new communications
 * and compare the received Merkle tree with its own version:
 * for each divergent node a bit equals to 0 is set in a BitSet object,
 * and sent to the source node.
 * After the end of the procedure the missing keys are sent.
*/
public class AntiEntropyReceiverThread extends AntiEntropyThread
{
	private TCPSession session;
	//private ExecutorService threadPool;
	
	/** Map used to manage the nodes in the synchronization phase */
	private static final HashMap<byte[], Integer> syncNodes = new HashMap<>();
	
	public AntiEntropyReceiverThread( final GossipMember _me,
									final DFSDatabase database,
									final FileManagerThread fMgr,
									final ConsistentHasherImpl<GossipMember, String> cHasher )
	{
		super( _me, database, fMgr, cHasher );
		
		//threadPool = Executors.newFixedThreadPool( QuorumSystem.getMaxNodes() );
		addToSynch( Utils.hexToBytes( me.getId() ).array() );
	}
	
	@Override
	public void run()
	{
		LOGGER.info( "Anti Entropy Receiver Thread launched" );
		String srcAddress;
		
		while(true) {
			byte[] sourceId = null;
			
			try {
				session = Net.waitForConnection( me.getHost(), port );
				
				srcAddress = session.getSrcAddress();
				
				// receive the first message from the source node
				ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
				// get the source node identifier
				sourceId = Utils.getNextBytes( data );
				GossipMember sourceNode = cHasher.getBucket( ByteBuffer.wrap( sourceId ) );
				if(sourceNode == null) {
					session.close();
					continue;
				}
				
				if(syncNodes.containsKey( sourceId )) {
					LOGGER.info( "Node " + Utils.bytesToHex( sourceId ) + " is syncronizing..." );
					session.close();
					continue;
				}
				
				byte msg_type = data.get();
				byte inputTree = data.get();
				
				LOGGER.debug( "TYPE: " + msg_type );
				List<DistributedFile> files;
				if(msg_type == MERKLE_FROM_MAIN) {
					ByteBuffer destId = ByteBuffer.wrap( sourceId );
					ByteBuffer fromId = cHasher.getPredecessor( destId );
					files = database.getKeysInRange( fromId, destId );
				}
				else {
					// get the virtual destination node identifier
					ByteBuffer destId = ByteBuffer.wrap( Utils.getNextBytes( data ) );
					ByteBuffer fromId = cHasher.getPredecessor( destId );
					if(fromId == null) fromId = cHasher.getLastKey();
					LOGGER.debug( "From: " + Utils.bytesToHex( fromId.array() ) + ", to: " + Utils.bytesToHex( destId.array() ) );
					files = database.getKeysInRange( fromId, destId );
				}
				
				List<DistributedFile> filesToSend = new ArrayList<>();
				m_tree = createMerkleTree( files );
				
				// check the differences through the trees
				boolean hasKey = checkTreeDifferences( inputTree );
				LOGGER.debug( "FROM_ID: " + Utils.bytesToHex( sourceId ) + ", GET_KEY: " + hasKey + ", TREE: " + m_tree + ", BIT_SET: " + bitSet );
				
				if(m_tree != null)
					filesToSend = getMissingFiles( files );
				
				addToSynch( sourceId );
				
				if(hasKey) {
					// receive the vector clocks associated to the shared files
					byte[] versions = session.receiveMessage();
					List<VectorClock> vClocks = getVersions( ByteBuffer.wrap( versions ) );
					filesToSend.addAll( checkVersions( sourceNode.getPort() + 1, files, vClocks, srcAddress, sourceId ) );
				}
				
				if(filesToSend.size() > 0)
					fMgr.sendFiles( sourceNode.getPort() + 1, Message.PUT, filesToSend, srcAddress, null, false, sourceId, false );
				else // no differences
					removeFromSynch( sourceId );
				
				session.close();
			}
			catch( IOException e ) {
				e.printStackTrace();
				
				if(sourceId != null)
					removeFromSynch( sourceId );
			}
		}
	}
	
	/**
	 * Computes the difference between the input and the own tree.
	 * 
	 * @param mTree		the input tree status (empty or not)
	 * 
	 * @return {@code true} if the soure node owns at least one of the keys,
	 * 		   {@code false} otherwise
	*/
	private boolean checkTreeDifferences( final byte mTree ) throws IOException
	{
		boolean sourceHasKey = false;
		
		LOGGER.debug( "My tree: " + m_tree + ", other: " + mTree );
		bitSet.clear();
		
		if(mTree == (byte) 0x0) {
			if(m_tree == null) // they are equals, also the "root"
				session.sendMessage( Networking.TRUE, false );
		}
		else {
			// tree not empty
			List<Node> nodes = new LinkedList<>();
			BitSet _bitSet = null;
			boolean clientOnLeaf = false;
			int height = 0;
			
			if(m_tree != null) {
				nodes.add( m_tree.getRoot() );
				height = m_tree.getHeight();
			}
			else {
				clientOnLeaf = true;
				_bitSet = new BitSet();
				_bitSet.set( 0 );
			}
			
			List<Node> pTree = null;
			
			int level = 0, size;
			LOGGER.debug( "Height: " + height );
			
			while((size = nodes.size()) > 0) {
				LOGGER.debug( "Level: " + level + ", NODES: " + size );
				
				if(!clientOnLeaf)
					_bitSet = new BitSet();
				
				if(!clientOnLeaf) {
					// receive a new level
					ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
					clientOnLeaf = (data.get() == (byte) 0x1);
					pTree = MerkleDeserializer.deserializeNodes( data );
					LOGGER.debug( "Received tree: " + pTree.size() );
				}
				
				LOGGER.debug( "On leaf: " + clientOnLeaf );
				
				sourceHasKey |= compareLevel( size, clientOnLeaf, level, height, nodes, _bitSet, pTree );
				
				if(!clientOnLeaf) {
					LOGGER.debug( "Sending not leaf: " + _bitSet + "..." );
					session.sendMessage( _bitSet.toByteArray(), true );
				}
				
				level++;
			}
			
			if(clientOnLeaf) {
				LOGGER.debug( "Sending on leaf: " + _bitSet + "..." );
				session.sendMessage( _bitSet.toByteArray(), true );
			}
		}
		
		return sourceHasKey;
	}
	
	/**
	 * Compares the current level with the input one.
	 * 
	 * @param size
	 * @param clientOnLeaf
	 * @param level
	 * @param height
	 * @param nodes
	 * @param _bitSet
	 * @param pTree
	 * 
	 * @return 
	*/
	private boolean compareLevel( int size, final boolean clientOnLeaf, final int level, final int height,
								  final List<Node> nodes, final BitSet _bitSet, final List<Node> pTree )
	{
		int pTreeSize = pTree.size();
		int offset = 0;
		boolean sourceHasKey = false;
		
		while(offset < size) {
			LOGGER.debug( "[SERVER] OFFSET: " + offset + ", SIZE: " + size );
			Node node = nodes.get( offset );
			boolean found = false;
			for(int j = 0; j < pTreeSize; j++) {
				// compare each signature: if equal put 1 in the set
				if(!_bitSet.get( j ) && MerkleDeserializer.signaturesEqual( node.sig, pTree.get( j ).sig )) {
					LOGGER.debug( "Founded 2 equal nodes!" );
					nodes.remove( offset );
					size--;
					_bitSet.set( j );
					
					if(!sourceHasKey)
						sourceHasKey = true;
					
					// set 1 to all the leaf nodes reachable from the node
					LinkedList<Node> leaves = m_tree.getLeavesFrom( node );
					LOGGER.debug( "From: " + leaves.getFirst().position + ", to: " + leaves.getLast().position );
					bitSet.set( leaves.getFirst().position, leaves.getLast().position + 1 );
					
					found = true;
					break;
				}
			}
			
			if(!found) {
				if(level < height) {
					//LOGGER.debug( "[SERVER] nodo " + offset + " rimosso" );
					nodes.remove( offset );
					size--;
					// adds the child nodes
					if(node.left != null) nodes.add( node.left );
					if(node.right != null) nodes.add( node.right );
				}
				else {
					if(clientOnLeaf) { // both are on the leaves level
						nodes.remove( offset );
						size--;
					}
					else
						offset++;
				}
			}
		}
		
		return sourceHasKey;
	}
	
	/**
	 * Gets all the files that the source doesn't have
	 * 
	 * @param files		list of files in the range
	*/
	private List<DistributedFile> getMissingFiles( final List<DistributedFile> files )
	{
		List<DistributedFile> filesToSend = new ArrayList<>();
		
		// flip the values
		bitSet.flip( 0, m_tree.getNumLeaves() );
		
		for(int i = bitSet.nextSetBit( 0 ); i >= 0; i = bitSet.nextSetBit( i+1 )) {
			if(i == Integer.MAX_VALUE)
				break; // or (i+1) would overflow
			else
				filesToSend.add( files.get( i ) );
		}
		
		// flip back the values
		bitSet.flip( 0, m_tree.getNumLeaves() );
		
		return filesToSend;
	}
	
	/**
	 * Checks the versions of the shared files.
	 * 
	 * @param port			destination port
	 * @param files			list of files in the range
	 * @param inClocks		source vector clocks
	 * @param address		source node address
	 * @param sourceNodeId	identifier of the source node
	 * 
	 * @return list of files which own an older version
	*/
	private List<DistributedFile> checkVersions( final int port,
												 final List<DistributedFile> files,
												 final List<VectorClock> inClocks,
												 final String address,
												 final byte[] sourceNodeId )
	{
		List<DistributedFile> filesToSend = new ArrayList<>();
		List<DistributedFile> filesToRemove = new ArrayList<>();
		
		// get the files that are shared by the two nodes, but with different versions
		for(int i = bitSet.nextSetBit( 0 ), j = 0; i >= 0; i = bitSet.nextSetBit( i+1 ), j++) {
			if(i == Integer.MAX_VALUE)
				break; // or (i+1) would overflow
			
			VectorClock vClock = inClocks.get( j );
			DistributedFile file = files.get( i );
			List<Versioned<Integer>> versions = Arrays.asList( new Versioned<Integer>( 0, vClock ),
															   new Versioned<Integer>( 1, file.getVersion() ) );
			
			// if the input version is older than mine, the associated file is added
			if(resolveVersions( versions ) == 1) {
				if(file.isDeleted())
					filesToRemove.add( files.get( i ) );
				else
					filesToSend.add( files.get( i ) );
			}
		}
		
		// send the files to delete
		if(filesToRemove.size() > 0) {
			addToSynch( sourceNodeId );
			fMgr.sendFiles( port, Message.DELETE, filesToRemove, address, null, false, sourceNodeId, false );
		}
		
		return filesToSend;
	}
	
	/**
	 * Resolve the (possible) inconsistency among the versions.
	 * 
	 * @param versions	list of versions
	 * 
	 * @return the value specified by the {@code T} type.
	*/
	private <T> T resolveVersions( final List<Versioned<T>> versions )
	{
		// get the list of concurrent versions
		VectorClockInconsistencyResolver<T> vecResolver = new VectorClockInconsistencyResolver<>();
		List<Versioned<T>> inconsistency = vecResolver.resolveConflicts( versions );
		
		// resolve the conflicts, using a time-based resolver
		TimeBasedInconsistencyResolver<T> resolver = new TimeBasedInconsistencyResolver<>();
		T id = resolver.resolveConflicts( inconsistency ).get( 0 ).getValue();
		
		return id;
	}
	
	private List<VectorClock> getVersions( final ByteBuffer versions )
	{
		List<VectorClock> vClocks = new ArrayList<>();
		while(versions.remaining() > 0) {
			VectorClock vClock = Utils.deserializeObject( Utils.getNextBytes( versions ) );
			vClocks.add( vClock );
		}
		
		return vClocks;
	}
	
	private synchronized void addToSynch( final byte[] nodeId )
	{
		Integer value = syncNodes.get( nodeId );
		if(value == null)
			syncNodes.put( nodeId, 1 );
		else
			syncNodes.put( nodeId, value + 1 );
	}
	
	/**
	 * Removes a node from the not yet synchronized ones.
	 * 
	 * @param nodeId	identifier of the node to remove
	*/
	public synchronized void removeFromSynch( final byte[] nodeId )
	{
		int value = syncNodes.get( nodeId );
		if(value == 1)
			syncNodes.remove( nodeId );
		else
			syncNodes.put( nodeId, value - 1 );
	}
}