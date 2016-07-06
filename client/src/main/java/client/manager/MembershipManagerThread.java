
package client.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipNode;

public class MembershipManagerThread extends Thread
{
    private final Random random;
    private final TCPnet net;
    private final DFSManager service;
    private List<GossipNode> members;
    private final ConsistentHasher<GossipMember, String> cHasher;
    
    private boolean closed = false;
    
    private static final Logger LOGGER = Logger.getLogger( MembershipManagerThread.class );
    private static final int TIMER_REQUEST = 10000; // 10 seconds.
    private static final int MAX_POOL_SIZE = 20; // Maximum number of nodes.
    
    
    
    public MembershipManagerThread( final TCPnet net,
                                    final DFSManager service,
                                    final ConsistentHasher<GossipMember, String> cHasher )
    {
        this.net = net;
        this.service = service;
        this.cHasher = cHasher;
        random = new Random();
        members = new ArrayList<>();
    }
    
    @Override
    public void run()
    {
        GossipMember server = null;
        while(!closed) {
            try {
                // The list of memebrs cannot be empty.
                List<GossipMember> members = cHasher.getAllBuckets();
                // If the list is empty it automatically switch in LoadBalancer mode.
                if(members.isEmpty()) {
                    LOGGER.info( "The list of nodes is empty. We are now using LoadBalancer nodes." );
                    service.setUseLoadBalancers( true );
                    break;
                }
                
                int index = random.nextInt( members.size() );
                server = members.get( index );
                LOGGER.debug( "[CLIENT] Selected: " + server );
                
                TCPSession session = net.tryConnect( server.getHost(), server.getPort() + 4, 2000 );
                if(session == null) {
                    cHasher.removeBucket( server );
                    LOGGER.debug( "[CLIENT] Node: " + server + " is unreachable." );
                }
                else {
                    // Receive the list of nodes.
                    MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
                    List<byte[]> nodes = message.getObjects();
                    
                    // Create the list of members.
                    if(nodes != null) {
                        List<GossipNode> remoteNodes = new ArrayList<>( nodes.size() );
                        for(byte[] node : nodes) {
                            GossipNode member = DFSUtils.deserializeObject( node );
                            if(member.getMember().getNodeType() == GossipMember.STORAGE)
                                remoteNodes.add( member );
                        }
                        
                        LOGGER.debug( "[CLIENT] Received: " + remoteNodes );
                        mergeLists( remoteNodes );
                        updateNodes();
                    }
                    
                    session.close();
                }
                
                sleep( TIMER_REQUEST );
            }
            catch( IOException e ) {
                // If the node is unreachable it will be removed.
                if(server != null)
                    removeNode( server );
            }
            catch( InterruptedException e ) {}
        }
    }
    
    /**
     * Merges the received list with the owned one.
     * 
     * @param remoteNodes   the remote list
    */
    private void mergeLists( final List<GossipNode> remoteNodes )
    {
        Set<GossipNode> nodeSet = new HashSet<>( members );
        nodeSet.addAll( remoteNodes );
        members = new ArrayList<>( nodeSet );
        Collections.sort( members );
        if(nodeSet.size() > MAX_POOL_SIZE)
            members = members.subList( 0, MAX_POOL_SIZE );
    }
    
    /**
     * Removes a node from the consistent hashing structure,
     * since it's not more reachable.
    */
    private void removeNode( final GossipMember node )
    {
        LOGGER.debug( "[CLIENT] Node: " + node + " is unreachable." );
        synchronized( cHasher ) {
            try { cHasher.removeBucket( node ); }
            catch( InterruptedException e ) {}
        }
    }
    
    /**
     * Updates the consistent hashing structure, putting the nodes on it.
    */
    private void updateNodes()
    {
        synchronized( cHasher ) {
            cHasher.clear();
            for(GossipNode node : members)
                cHasher.addBucket( node.getMember(), node.getMember().getVirtualNodes() );
        }
    }
    
    /**
     * Wakes up the node to start immediately
     * the membership poll request.
    */
    public void wakeUp()
    {
        interrupt();
    }
    
    public void close()
    {
        closed = true;
        interrupt();
    }
}