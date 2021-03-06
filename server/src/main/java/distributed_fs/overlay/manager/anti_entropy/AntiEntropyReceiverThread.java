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
import distributed_fs.net.Networking.Session;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.overlay.manager.anti_entropy.MerkleTree.Node;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransfer;
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
    private final int port;
    
    /** Map used to manage nodes in the synchronization phase */
    private volatile Set<String> syncNodes = new ConcurrentSkipListSet<>();
    
    
    
    
    public AntiEntropyReceiverThread( GossipMember _me,
                                      DFSDatabase database,
                                      FileTransfer fMgr,
                                      ConsistentHasher<GossipMember, String> cHasher )
    {
        super( _me, database, fMgr, cHasher );
        setName( "AntiEntropyReceiver" );
        
        threadPool = Executors.newFixedThreadPool( QuorumSession.getMaxNodes() );
        addToSynch( me.getId() );
        net.setSoTimeout( 500 );
        
        port = me.getPort() + PORT_OFFSET;
    }
    
    @Override
    public void run()
    {
        LOGGER.info( "Anti Entropy Receiver Thread launched" );
        LOGGER.info( "[AE] Waiting on: " + me.getHost() + ":" + port );
        
        while(!shutDown.get()) {
            try {
                Session session = net.waitForConnection( me.getHost(), port );
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
        private Session session;
        private final List<DistributedFile> filesToSend;
        private String sourceId = null;
        private BitSet bitSet = new BitSet();
        
        public AntiEntropyNode( Session session )
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
                
                String prevId = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
                int sourcePort = DFSUtils.byteArrayToInt( DFSUtils.getNextBytes( data ) );
                
                // Get the input tree status and height.
                byte inputTree = data.get();
                int inputHeight = (inputTree == (byte) 0x0) ?
                                  0 : DFSUtils.byteArrayToInt( DFSUtils.getNextBytes( data ) );
                
                //fromId = cHasher.getPreviousBucket( sourceId );
                List<DistributedFile> files = database.getKeysInRange( prevId, sourceId );
                LOGGER.debug( "FROM: " + session.getEndPointAddress() + ":" + sourcePort + 
                              ", FromDHT: " + prevId +
                              ", toDHT: " + sourceId +
                              ", FILES: " + files );
                
                m_tree = createMerkleTree( files );
                
                //System.out.println( "[RCV] FROM: " + cHasher.getBucket( sourceId ).getPort() + ", ME: " + me.getPort() + ", Files: " + files );
                checkTreeDifferences( inputTree, inputHeight );
                
                LOGGER.debug( "FROM: " + session.getEndPointAddress() + ":" + sourcePort + 
                              ", FromDHT: " + prevId +
                              ", toDHT: " + sourceId +
                              ", BIT_SET: " + bitSet );
                
                if(m_tree != null)
                    getMissingFiles( files );
                
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
                e.printStackTrace();
                
                if(sourceId != null)
                    removeFromSynch( sourceId );
            }
            
            session.close();
        }
        
        /**
         * Starts the handshake phase.
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
            
            // Send out a positive response.
            session.sendMessage( Networking.TRUE, false );
            // Send the own port.
            session.sendMessage( DFSUtils.intToByteArray( me.getPort() ), false );
            return data;
        }
        
        /**
         * Computes the difference between the input and the own tree.
         * 
         * @param inputTree        the input tree status (empty or not)
         * @param inputHeight    the height of the input tree
        */
        private void checkTreeDifferences( byte inputTree, int inputHeight ) throws IOException
        {
            bitSet.clear();
            
            if(inputTree == (byte) 0x1) { // Tree not empty.
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
                
                LOGGER.debug( "Height: " + treeHeight );
                
                List<Node> pTree = null;
                for(int levels = Math.min( treeHeight, inputHeight ); levels >= 0 && nodes.size() > 0; levels--) {
                    // Receive a new level.
                    ByteBuffer data = ByteBuffer.wrap( session.receive() );
                    pTree = MerkleDeserializer.deserializeNodes( data );
                    LOGGER.debug( "Received tree: " + pTree.size() );
                    
                    if(compareLevel( nodes, pTree ))
                        break;
                }
            }
        }
        
        /**
         * Compares the current level with the input one.
         * 
         * @param nodes        list of own nodes
         * @param pTree        the input tree level
         *
         * @return {@code true} if the input level is equal with the current one, {@code false} otherwise
        */
        private boolean compareLevel( Deque<Node> nodes, List<Node> pTree ) throws IOException
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
                    // Compare the current node with the correspondent input signature:
                    // if equals put 1 in the set.
                    if(MerkleDeserializer.signaturesEqual( node.sig, pTree.get( i ).sig )) {
                        _bitSet.set( i );
                        found = true;
                    }
                }
                else {
                    // Compare the current node with each input signature:
                    // if equals put 1 in the set.
                    for(int j = index + 1; j < pTreeSize; j++) {
                        if(MerkleDeserializer.signaturesEqual( node.sig, pTree.get( j ).sig )) {
                            _bitSet.set( index = j );
                            found = true;
                            break;
                        }
                    }
                }
                
                if(found) {
                    // Set 1 all the leaves reachable from the node.
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
            
            byte[] msg = net.createMessage( new byte[]{ (byte) ((nodes.size() == 0) ? 0x1 : 0x0 ) }, _bitSet.toByteArray(), false );
            session.sendMessage( msg, true );
            
            return _bitSet.cardinality() == pTreeSize;
        }
        
        /**
         * Gets all the files that the source doesn't have.
         * 
         * @param files        list of files in the range
        */
        private void getMissingFiles( List<DistributedFile> files ) throws IOException
        {
            // Flip the values.
            bitSet.flip( 0, m_tree.getNumLeaves() );
            
            for(int i = bitSet.nextSetBit( 0 ); i >= 0; i = bitSet.nextSetBit( i+1 )) {
                DistributedFile file = files.get( i );
                file.loadContent( database );
                filesToSend.add( file );
                //System.out.println( "Sending: " + file + " because absent." );
                if(i == Integer.MAX_VALUE)
                    break; // or (i+1) would overflow.
            }
            
            // Flip back the values.
            bitSet.flip( 0, m_tree.getNumLeaves() );
        }
        
        private List<VectorClock> getVersions( ByteBuffer versions )
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
         * @param port            destination port
         * @param files            list of own files in the range
         * @param inClocks        source vector clocks
         * @param address        source node address
         * @param sourceNodeId    identifier of the source node
        */
        private void checkVersions( int port,
                                    List<DistributedFile> files,
                                    List<VectorClock> inClocks,
                                    String address,
                                    String sourceNodeId ) throws IOException
        {
            BitSet filesToReceive = new BitSet();
            
            // Get the files that are shared by the two nodes, but with different versions.
            for(int i = bitSet.nextSetBit( 0 ), j = 0; i >= 0; i = bitSet.nextSetBit( i+1 ), j++) {
                if(i == Integer.MAX_VALUE)
                    break; // or (i+1) would overflow.
                
                DistributedFile file = files.get( i );
                Occurred result = inClocks.get( j ).compare( file.getVersion() );
                // If the own file has the most updated version, it is sent.
                if(result == Occurred.BEFORE) {
                    file.loadContent( database );
                    filesToSend.add( file );
                }
                else if(result == Occurred.AFTER) {
                    // The received file has the most updated version.
                    filesToReceive.set( j );
                }
            }
            
            // Send the list of versions to retrieve.
            session.sendMessage( filesToReceive.toByteArray(), true );
        }
    }
    
    private boolean isSynch( String nodeId )
    {
        return syncNodes.contains( nodeId );
    }
    
    private void addToSynch( String nodeId )
    {
        syncNodes.add( nodeId );
    }
    
    /**
     * Removes a node from the not yet synchronized ones.
     * 
     * @param nodeId    identifier of the node to remove
    */
    public void removeFromSynch( String nodeId )
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
            threadPool.awaitTermination( WAIT_TIMER, TimeUnit.SECONDS );
        } catch( InterruptedException e1 ) {
            e1.printStackTrace();
        }
    }
}
