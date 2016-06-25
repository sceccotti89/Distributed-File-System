
package distributed_fs.client.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

public class ListManagerThread extends Thread
{
    private final Random random;
    private final TCPnet net;
    private final ConsistentHasherImpl<GossipMember, String> cHasher;
    
    private boolean closed = false;
    
    private static final Logger LOGGER = Logger.getLogger( ListManagerThread.class );
    private static final int TIMER_REQUEST = 10000;
    
    public ListManagerThread( final TCPnet net,
                              final ConsistentHasherImpl<GossipMember, String> cHasher )
    {
        this.net = net;
        this.cHasher = cHasher;
        random = new Random();
    }
    
    @Override
    public void run()
    {
        while(!closed) {
            try {
                // The list of memebrs cannot be empty.
                List<GossipMember> members = cHasher.getAllBuckets();
                int index = random.nextInt( members.size() );
                GossipMember server = members.get( index );
                LOGGER.debug( "[CLIENT] Selected: " + server );
                
                TCPSession session = net.tryConnect( server.getHost(), server.getPort() + 4 );
                
                // Receive the list of nodes.
                MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
                List<byte[]> nodes = message.getObjects();
                
                // Create the list of members.
                List<GossipMember> servers = new ArrayList<>( nodes.size() );
                for(byte[] node : nodes) {
                    GossipMember member = DFSUtils.deserializeObject( node );
                    if(member.getNodeType() == GossipMember.STORAGE)
                        servers.add( member );
                }
                
                if(servers != null) {
                    LOGGER.debug( "[CLIENT] received: " + servers );
                    // Update the consistent hashing structure, putting the nodes on it.
                    synchronized( cHasher ) {
                        // TODO usare questo metodo?
                        // TODO oppure usare una tecnica un po' piu' sofisticata
                        //cHasher.clear();
                        for(GossipMember member : servers)
                            cHasher.addBucket( member, member.getVirtualNodes() );
                    }
                }
                
                session.close();
            }
            catch( IOException e ){
                e.printStackTrace();
            }
            
            try { sleep( TIMER_REQUEST ); }
            catch( InterruptedException e ) { break; }
        }
    }
    
    public void close()
    {
        closed = true;
        interrupt();
    }
}