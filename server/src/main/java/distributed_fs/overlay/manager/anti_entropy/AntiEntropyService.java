/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager.anti_entropy;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.FileTransfer;
import gossiping.GossipMember;

/**
 * Class used to start in a fully automatic way the anti-entropy protocol.<br>
 * It manages the start and stop of the Threads used to perform the mechanism.
*/
public class AntiEntropyService
{
    private AntiEntropySenderThread sendAE_t;
    private AntiEntropyReceiverThread receiveAE_t;
    
    private final ConsistentHasher<GossipMember, String> _cHasher;
    private final GossipMember _node;
    private final FileTransfer _fTransfer;
    private final DFSDatabase _db;
    
    private boolean disabledAntiEntropy = false;
    
    
    
    public AntiEntropyService( final GossipMember node,
                               final FileTransfer fTransfer,
                               final DFSDatabase db,
                               final ConsistentHasher<GossipMember, String> cHasher )
    {
        _cHasher = cHasher;
        _node = node;
        _fTransfer = fTransfer;
        _db = db;
        
        sendAE_t = new AntiEntropySenderThread( node, db, fTransfer, cHasher );
        receiveAE_t = new AntiEntropyReceiverThread( node, db, fTransfer, cHasher );
    }
    
    /**
     * Enable/disable the anti-entropy mechanism.<br>
     * By default this value is setted to {@code true}.
     * 
     * @param enable    {@code true} to enable the anti-entropy mechanism,
     *                  {@code false} otherwise
    */
    public void setAntiEntropy( final boolean enable )
    {
        if(disabledAntiEntropy == !enable)
            return;
        disabledAntiEntropy = !enable;
        
        if(!disabledAntiEntropy) {
            sendAE_t = new AntiEntropySenderThread( _node, _db, _fTransfer, _cHasher );
            receiveAE_t = new AntiEntropyReceiverThread( _node, _db, _fTransfer, _cHasher );
        }
        else {
            // Close the background Anti-Entropy Threads.
            shutDown();
        }
    }
    
    public AntiEntropyReceiverThread getReceiver()
    {
        return receiveAE_t;
    }
    
    public void start()
    {
        receiveAE_t.start();
        sendAE_t.start();
    }
    
    public void shutDown()
    {
        sendAE_t.close();
        receiveAE_t.close();
        
        try {
            sendAE_t.join();
            receiveAE_t.join();
        } catch( InterruptedException e ){ e.printStackTrace(); }
    }
}