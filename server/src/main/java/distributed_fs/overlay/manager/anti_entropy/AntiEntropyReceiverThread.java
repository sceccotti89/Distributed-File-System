/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager.anti_entropy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.overlay.manager.FileTransferThread;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.overlay.manager.anti_entropy.MerkleTree.Node;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.Occurred;
import distributed_fs.versioning.VectorClock;
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
	
	/** Map used to manage nodes in the synchronization phase */
	private final Set<String> syncNodes = new ConcurrentSkipListSet<>();
	
	
	
	
	public AntiEntropyReceiverThread( final GossipMember _me,
	                                  final DFSDatabase database,
	                                  final FileTransferThread fMgr,
	                                  final ConsistentHasher<GossipMember, String> cHasher )
	{
	    super( _me, database, fMgr, cHasher );
	    setName( "AntiEntropyReceiver" );
	    
	    threadPool = Executors.newFixedThreadPool( QuorumSession.getMaxNodes() );
	    addToSynch( me.getId() );
	    net.setSoTimeout( 500 );
	}
	
	@Override
	public void run()
	{
	    LOGGER.info( "Anti Entropy Receiver Thread launched" );
	    LOGGER.info( "[AE] Waiting on: " + me.getHost() + ":" + (me.getPort() + PORT_OFFSET) );
	    
	    while(!shutDown.get()) {
	        try {
	            TCPSession session = net.waitForConnection( me.getHost(), me.getPort() + PORT_OFFSET );
	            if(session == null)
	                continue;
	            
	            synchronized ( threadPool ) {
	                if(threadPool.isShutdown())
                        break;
	                
	                threadPool.execute( new AntiEntropyNode( session ) );
                }
	        }
	        catch( IOException e ) {
	            e.printStackTrace();
	        }
	    }
	    
	    LOGGER.info( "Anti-entropy Receiver Thread closed." );
	}
	
	/**
	 * Class used to manage an incoming connection.<br>
	 * It controls all the phases of the process,
	 * from the handshake, passing from the comparison of the trees,
	 * ending with the versions resolution.
	*/
	private class AntiEntropyNode extends Thread
	{
		private MerkleTree m_tree = null;
		private TCPSession session;
		private final List<DistributedFile> filesToSend;
		private int sourcePort;
		private String sourceId = null;
		private BitSet bitSet = new BitSet();
		
		public AntiEntropyNode( final TCPSession session )
		{
		    setName( "AntiEntropyNode" );
		    
			this.session = session;
			filesToSend = new ArrayList<>();
		}
		
		@Override
		public void run()
		{
			try {
				String srcAddress = session.getEndPointAddress();
				
				ByteBuffer data = handshake();
				if(data == null)
					return;
				
				// Get the input tree status and height.
				byte inputTree = data.get();
				int inputHeight = (inputTree == (byte) 0x0) ?
								  0 : DFSUtils.byteArrayToInt( DFSUtils.getNextBytes( data ) );
				
			    String fromId = cHasher.getPreviousBucket( sourceId );
			    List<DistributedFile> files = database.getKeysInRange( fromId, sourceId );
				LOGGER.debug( "MAIN - Node: " + me.getPort() +
				              ", From: " + cHasher.getBucket( fromId ).getAddress() +
				              ", to: " + cHasher.getBucket( sourceId ).getAddress() +
				              ", FILES: " + files );
				
				m_tree = createMerkleTree( files );
				
				//System.out.println( "[RCV] FROM: " + cHasher.getBucket( sourceId ).getPort() + ", ME: " + me.getPort() + ", Files: " + files );
				// Check the differences through the trees.
				checkTreeDifferences( inputTree, inputHeight );
				
				LOGGER.debug( "FROM_ID: " + sourceId + ", TREE: " + m_tree + ", BIT_SET: " + bitSet );
				
				if(m_tree != null)
					getMissingFiles( files );
				//System.out.println( "FROM: " + cHasher.getBucket( sourceId ).getPort() + ", ME: " + me.getPort() + ", MISSING FILES: " + filesToSend );
				
				if(bitSet.cardinality() > 0) {
					// Receive the vector clocks associated to the common files.
					byte[] versions = session.receive();
					List<VectorClock> vClocks = getVersions( ByteBuffer.wrap( versions ) );
					checkVersions( sourcePort, files, vClocks, srcAddress, sourceId );
				}
				
				if(filesToSend.size() > 0) {
				    addToSynch( sourceId );
					fMgr.sendFiles( srcAddress, sourcePort, filesToSend, false, sourceId, null );
				}
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
			ByteBuffer data = ByteBuffer.wrap( session.receive() );
			// Get the source node identifier.
			sourceId = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
			if(isSynch( sourceId )) {
                LOGGER.info( "Node " + sourceId + " is synchronizing..." );
                // Send out a negative response.
                session.sendMessage( Networking.FALSE, false );
                session.close();
                return null;
            }
			
			sourcePort = DFSUtils.byteArrayToInt( DFSUtils.getNextBytes( data ) );
			
			// Send out a positive response.
            session.sendMessage( Networking.TRUE, false );
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
			
			if(inputTree == (byte) 0x1) {
			    // Tree not empty.
			    Deque<Node> nodes = new ArrayDeque<>( (m_tree == null) ? 0 : m_tree.getNumNodes() );
				int treeHeight = 0;
				
				if(m_tree != null) {
					nodes.add( m_tree.getRoot() );
					treeHeight = m_tree.getHeight();
				}
				
				// Send the own tree height to the sender node.
				byte[] msg = DFSUtils.intToByteArray( treeHeight );
				session.sendMessage( msg, false );
				
				// Reduce the level of the tree if it is greater.
				if(treeHeight > inputHeight)
					reduceTree( treeHeight - inputHeight, nodes );
				
				List<Node> pTree = null;
				
				LOGGER.debug( "Height: " + treeHeight );
				
				for(int levels = Math.min( treeHeight, inputHeight ); levels > 0 && nodes.size() > 0; levels--) {
					// Receive a new level.
					ByteBuffer data = ByteBuffer.wrap( session.receive() );
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
		private void compareLevel( final Deque<Node> nodes, final List<Node> pTree ) throws IOException
		{
			BitSet _bitSet = new BitSet();
			int pTreeSize = pTree.size();
			int nodeSize = nodes.size();
			boolean equalLevel = (nodeSize == pTree.size());
			int index = -1; // Index used to scan efficiently the tree.
			
			for(int i = 0; i < nodeSize; i++) {
			    Node node = nodes.removeFirst();
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
				
				if(found) {
				    // Set 1 all the range reachable from the node.
				    Deque<Node> leaves = m_tree.getLeavesFrom( node );
                    LOGGER.debug( "From: " + leaves.getFirst().position + ", to: " + leaves.getLast().position );
                    bitSet.set( leaves.getFirst().position, leaves.getLast().position + 1 );
				}
				else {
					// If the current node is not found, its sons will be added.
				    if(node.left  != null){
				        nodes.addLast( node.left );
				        if(node.right != null)
				            nodes.addLast( node.right );
				    }
				}
			}
			
			session.sendMessage( _bitSet.toByteArray(), true );
		}
		
		/**
		 * Gets all the files that the source doesn't have.
		 * 
		 * @param files		list of files in the range
		*/
		private void getMissingFiles( final List<DistributedFile> files )
		{
			// Flip the values.
			bitSet.flip( 0, m_tree.getNumLeaves() );
			
			for(int i = bitSet.nextSetBit( 0 ); i >= 0; i = bitSet.nextSetBit( i+1 )) {
			    filesToSend.add( files.get( i ) );
				if(i == Integer.MAX_VALUE)
					break; // or (i+1) would overflow
			}
			
			// Flip back the values.
			bitSet.flip( 0, m_tree.getNumLeaves() );
		}
		
		private List<VectorClock> getVersions( final ByteBuffer versions )
        {
            int size = bitSet.cardinality();
        	List<VectorClock> vClocks = new ArrayList<>( size );
        	for(int i = 0; i < size; i++) {
        		VectorClock vClock = DFSUtils.deserializeObject( DFSUtils.getNextBytes( versions ) );
        		vClocks.add( vClock );
        	}
        	
        	return vClocks;
        }

        /**
		 * Checks the versions of the shared files.
		 * 
		 * @param port			destination port
		 * @param files			list of own files in the range
		 * @param inClocks		source vector clocks
		 * @param address		source node address
		 * @param sourceNodeId	identifier of the source node
		*/
		private void checkVersions( final int port,
                                    final List<DistributedFile> files,
                                    final List<VectorClock> inClocks,
		                            final String address,
		                            final String sourceNodeId )
		{
			// Get the files that are shared by the two nodes, but with different versions.
			for(int i = bitSet.nextSetBit( 0 ), j = 0; i >= 0; i = bitSet.nextSetBit( i+1 ), j++) {
				if(i == Integer.MAX_VALUE)
					break; // or (i+1) would overflow
				
				DistributedFile file = files.get( i );
				VectorClock vClock = inClocks.get( j );
				
				// If the input version is older than mine, the associated file is added.
				if(!(file.getVersion().compare( vClock ) == Occurred.BEFORE)) 
					filesToSend.add( files.get( i ) );
			}
		}
	}
	
	private boolean isSynch( final String nodeId )
	{
	    return syncNodes.contains( nodeId );
	}
	
	private void addToSynch( final String nodeId )
	{
	    syncNodes.add( nodeId );
	}
	
	/**
	 * Removes a node from the not yet synchronized ones.
	 * 
	 * @param nodeId	identifier of the node to remove
	*/
	public void removeFromSynch( final String nodeId )
	{
		syncNodes.remove( nodeId );
	}
	
	@Override
	public void close()
	{
	    super.close();
	    
	    synchronized( threadPool ) {
            threadPool.shutdown();
        }
        
        try {
            threadPool.awaitTermination( EXCH_TIMER, TimeUnit.MILLISECONDS );
        } catch( InterruptedException e1 ) {
            e1.printStackTrace();
        }
	}
}
