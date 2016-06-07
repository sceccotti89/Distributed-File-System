/**
 * @author Stefano Ceccotti
*/

package distributed_fs.anti_entropy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import distributed_fs.anti_entropy.MerkleTree.Node;
import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.overlay.manager.QuorumSession;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransferThread;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.VersioningUtils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
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
	private ExecutorService threadPool;
	
	/** Map used to manage the nodes in the synchronization phase */
	private static final Map<byte[], Integer> syncNodes = new ConcurrentHashMap<>( 8 );
	
	public AntiEntropyReceiverThread( final GossipMember _me,
									  final DFSDatabase database,
									  final FileTransferThread fMgr,
									  final ConsistentHasherImpl<GossipMember, String> cHasher ) throws IOException
	{
		super( _me, database, fMgr, cHasher );
		
		threadPool = Executors.newFixedThreadPool( QuorumSession.getMaxNodes() );
		addToSynch( DFSUtils.hexToBytes( me.getId() ) );
		Net.setSoTimeout( 2000 );
	}
	
	@Override
	public void run()
	{
		LOGGER.info( "Anti Entropy Receiver Thread launched" );
		
		while(!shoutDown) {
			try {
				//System.out.println( "[AE] Waiting on: " + me.getHost() + ":" + port );
				TCPSession session = Net.waitForConnection( me.getHost(), me.getPort() + 2 );
				if(session == null)
					continue;
				threadPool.execute( new AntiEntropyNode( session ) );
			}
			catch( IOException e ) {
				e.printStackTrace();
			}
		}
		
		LOGGER.info( "Anti-entropy Receiver Thread closed." );
	}
	
	private class AntiEntropyNode extends Thread
	{
		private MerkleTree m_tree = null;
		private TCPSession session;
		private GossipMember sourceNode;
		private byte[] sourceId = null;
		private BitSet bitSet = new BitSet();
		
		public AntiEntropyNode( final TCPSession session )
		{
			this.session = session;
		}
		
		@Override
		public void run()
		{
			try {
				String srcAddress = session.getSrcAddress();
				
				ByteBuffer data = handshake();
				if(data == null)
					return;
				
				byte msg_type = data.get();
				
				// Get the input tree status and height.
				byte inputTree = data.get();
				int inputHeight = (inputTree == (byte) 0x0) ?
								  0 : DFSUtils.byteArrayToInt( DFSUtils.getNextBytes( data ) );
				
				LOGGER.debug( "TYPE: " + msg_type );
				List<DistributedFile> files;
				if(msg_type == MERKLE_FROM_MAIN) {
					ByteBuffer destId = ByteBuffer.wrap( sourceId );
					ByteBuffer fromId = cHasher.getPreviousBucket( destId );
					files = database.getKeysInRange( fromId, destId );
					LOGGER.debug( "From: " + cHasher.getBucket( fromId ).getAddress() + ", to: " + cHasher.getBucket( destId ).getAddress() );
				}
				else {
					// Get the virtual destination node identifier.
					ByteBuffer destId = ByteBuffer.wrap( DFSUtils.getNextBytes( data ) );
					ByteBuffer fromId = cHasher.getPreviousBucket( destId );
					LOGGER.debug( "From: " + cHasher.getBucket( fromId ).getAddress() + ", to: " + cHasher.getBucket( destId ).getAddress() );
					files = database.getKeysInRange( fromId, destId );
				}
				
				// ================ TODO finiti i test, togliere sta roba ================= //
				//System.out.println( me + " - RICEVUTA CONNESSIONE DAL NODO CHE MI INTERESSA!!!!!!!!!!!!!!!!!" );
				LOGGER.debug( "Files: " + files );
				// =========================================================================== //
				
				List<DistributedFile> filesToSend = new ArrayList<>();
				// TODO si potrebbe usare una variabile che determina fino a che altezza creare l'albero
				// TODO per esempio il minimo tra le due altezze
				m_tree = createMerkleTree( files );
				
				// Check the differences through the trees.
				checkTreeDifferences( inputTree, inputHeight );
				LOGGER.debug( "FROM_ID: " + DFSUtils.bytesToHex( sourceId ) + ", TREE: " + m_tree + ", BIT_SET: " + bitSet );
				
				if(m_tree != null)
					filesToSend = getMissingFiles( files );
				
				addToSynch( sourceId );
				
				if(bitSet.cardinality() > 0) {
					// Receive the vector clocks associated to the shared files.
					byte[] versions = session.receiveMessage();
					List<VectorClock> vClocks = getVersions( ByteBuffer.wrap( versions ) );
					filesToSend.addAll( checkVersions( sourceNode.getPort() + 1, files, vClocks, srcAddress, sourceId ) );
				}
				
				if(filesToSend.size() > 0)
					fMgr.sendFiles( sourceNode.getPort() + 1/*, Message.PUT*/, filesToSend, srcAddress, false, sourceId, null );
				else // No differences.
					removeFromSynch( sourceId );
			}
			catch( IOException e ) {
			    // Ignored.
				//e.printStackTrace();
				
				if(sourceId != null)
					removeFromSynch( sourceId );
			}
			
			session.close();
		}
		
		/**
		 * Start the handshake phase.
		 * During this phase some important informations
		 * are exchanged.
		*/
		private ByteBuffer handshake() throws IOException
		{
			ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
			// Get the source node identifier.
			sourceId = DFSUtils.getNextBytes( data );
			sourceNode = cHasher.getBucket( ByteBuffer.wrap( sourceId ) );
			if(sourceNode == null) {
				session.close();
				return null;
			}
			
			//LOGGER.debug( "Received connection from: " + srcAddress + ", Id: " + Utils.bytesToHex( sourceId ) );
			
			if(syncNodes.containsKey( sourceId )) {
				LOGGER.info( "Node " + DFSUtils.bytesToHex( sourceId ) + " is syncronizing..." );
				session.close();
				return null;
			}
			
			return data;
		}
		
		/**
		 * Computes the difference between the input and the own tree.
		 * 
		 * @param inputTree		the input tree status (empty or not)
		 * @param inputHeight	the height of the input tree
		*/
		private void checkTreeDifferences( final byte inputTree, final int inputHeight ) throws IOException
		{
			LOGGER.debug( "My tree: " + m_tree + ", other: " + inputTree );
			bitSet.clear();
			
			if(inputTree == (byte) 0x1) { // Tree not empty.
				List<Node> nodes = new LinkedList<>();
				int treeHeight = 0;
				
				if(m_tree != null) {
					nodes.add( m_tree.getRoot() );
					treeHeight = m_tree.getHeight();
				}
				
				// Send the own tree height to the sender node.
				byte[] msg = Net.createMessage( null, DFSUtils.intToByteArray( treeHeight ), true );
				session.sendMessage( msg, false );
				
				// Reduce the level of the tree if it is greater.
				if(treeHeight > inputHeight)
					reduceTree( treeHeight - inputHeight, nodes );
				
				List<Node> pTree = null;
				
				LOGGER.debug( "Height: " + treeHeight );
				
				for(int levels = Math.min( treeHeight, inputHeight ); levels > 0 && nodes.size() > 0; levels--) {
					// Receive a new level.
					ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
					pTree = MerkleDeserializer.deserializeNodes( data );
					LOGGER.debug( "Received tree: " + pTree.size() );
					
					compareLevel( nodes, pTree );
				}
			}
		}
		
		/**
		 * Compares the current level with the input one.
		 * 
		 * @param nodes		list of own nodes
		 * @param pTree		the input tree level
		*/
		private void compareLevel( final List<Node> nodes, final List<Node> pTree ) throws IOException
		{
			BitSet _bitSet = new BitSet();
			int pTreeSize = pTree.size();
			int nodeSize = nodes.size();
			boolean equalLevel = nodeSize == pTree.size();
			int index = -1; // Index used to scan efficiently the tree.
			
			ListIterator<Node> it = nodes.listIterator();
			for(int i = 0; i < nodeSize; i++) {
				Node node = it.next();
				boolean found = false;
				
				if(equalLevel) {
				    // Compare the current node with the correspondent input signature: if equals put 1 in the set.
				    if(MerkleDeserializer.signaturesEqual( node.sig, pTree.get( i ).sig )) {
                        _bitSet.set( i );
                        found = true;
                    }
				}
				else {
				    // Compare the current node with each input signature: if equals put 1 in the set.
    				for(int j = index + 1; j < pTreeSize; j++) {
    					if(MerkleDeserializer.signaturesEqual( node.sig, pTree.get( j ).sig )) {
    						_bitSet.set( index = j );
    						found = true;
    						break;
    					}
    				}
				}
				
				it.remove();
				
				if(found) {
				    // Set 1 all the range reachable from the node.
                    LinkedList<Node> leaves = m_tree.getLeavesFrom( node );
                    LOGGER.debug( "From: " + leaves.getFirst().position + ", to: " + leaves.getLast().position );
                    bitSet.set( leaves.getFirst().position, leaves.getLast().position + 1 );
				}
				else {
					// If the current node is not found, its sons will be added.
				    if(node.left  != null) it.add( node.left );
				    if(node.right != null) it.add( node.right );
				}
			}
			
			session.sendMessage( _bitSet.toByteArray(), true );
		}
		
		/**
		 * Gets all the files that the source doesn't have.
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
				fMgr.sendFiles( port/*, Message.DELETE*/, filesToRemove, address, false, sourceNodeId, null );
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
			//VectorClockInconsistencyResolver<T> vecResolver = new VectorClockInconsistencyResolver<>();
			//List<Versioned<T>> inconsistency = vecResolver.resolveConflicts( versions );
			List<Versioned<T>> inconsistency = VersioningUtils.resolveVersions( versions );
			
			// resolve the conflicts, using a time-based resolver
			TimeBasedInconsistencyResolver<T> resolver = new TimeBasedInconsistencyResolver<>();
			T id = resolver.resolveConflicts( inconsistency ).get( 0 ).getValue();
			
			return id;
		}
		
		private List<VectorClock> getVersions( final ByteBuffer versions )
		{
			List<VectorClock> vClocks = new ArrayList<>();
			while(versions.remaining() > 0) {
				VectorClock vClock = DFSUtils.deserializeObject( DFSUtils.getNextBytes( versions ) );
				vClocks.add( vClock );
			}
			
			return vClocks;
		}
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