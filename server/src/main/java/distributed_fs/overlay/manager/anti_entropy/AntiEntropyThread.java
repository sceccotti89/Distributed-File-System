/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager.anti_entropy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.overlay.manager.anti_entropy.MerkleTree.Node;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransfer;
import gossiping.GossipMember;

/**
 * Abstract class used to execute the anti-entropy machanism,
 * performed with the Merkle tree exchanging protocol.
*/
public abstract class AntiEntropyThread extends Thread
{
    protected final ConsistentHasher<GossipMember, String> cHasher;
    protected final FileTransfer fMgr;
    protected final DFSDatabase database;
    protected final TCPnet net;
    protected final GossipMember me;
    private final Random random;
    protected AtomicBoolean shutDown = new AtomicBoolean( false );
    
    protected static final int PORT_OFFSET = 3;
    
    /** Updating timer. */
    public static final int WAIT_TIMER = 5; // 5 seconds.
    
    /** Logger used to print the application state. */
    protected static final Logger LOGGER = Logger.getLogger( AntiEntropyThread.class.getName() );
    
    
    
    
    
    public AntiEntropyThread( final GossipMember _me,
                              final DFSDatabase database,
                              final FileTransfer fMgr,
                              final ConsistentHasher<GossipMember, String> _cHasher )
    {
        setName( "AntiEntropy" );
        
        this.me = _me;
        this.fMgr = fMgr;
        this.database = database;
        net = new TCPnet();
        cHasher = _cHasher;
        random = new Random();
    }
    
    /** 
     * Creates the Merkle tree corresponding to the input files.
     * 
     * @param files        files used to create the Merkle tree
    */
    protected MerkleTree createMerkleTree( final List<DistributedFile> files )
    {
        if(files == null || files.size() == 0)
            return null;
        
        List<byte[]> bytes = new ArrayList<>( files.size() );
        for(DistributedFile file : files)
            bytes.add( file.getSignature() );
        
        return new MerkleTree( bytes );
    }
    
    /**
     * Reduces the level of the tree until the requested level is not reached.
     * 
     * @param levels    number of levels to reduce
     * @param nodes        list of nodes
    */
    protected void reduceTree( final int levels, final Deque<Node> nodes )
    {
        for(int i = 0; i < levels; i++) {
            int size = nodes.size();
            for(int j = 0; j < size; j++) {
                Node n = nodes.removeFirst();
                if(n.left != null) nodes.addLast( n.left );
                if(n.right != null) nodes.addLast( n.right );
            }
        }
    }
    
    /**
     * [The selectToSend() function.] Find a random peer from the local
     * membership list. In the case where this client is the only member in the
     * list, this method will return null.
     *
     * @return Index of the random member if list is greater than 1, -1 otherwise
    */
    protected int selectPartner( final List<String> memberList ) 
    {
        int randomNeighborIndex = -1;
        if (memberList.size() > 0)
            randomNeighborIndex = random.nextInt( memberList.size() );
        
        return randomNeighborIndex;
    }
    
    public void close()
    {
        shutDown.set( true );
        interrupt();
    }
}
