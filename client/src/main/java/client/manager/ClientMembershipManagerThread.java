/**
 * @author Stefano Ceccotti
*/

package client.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.net.Networking.Session;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.manager.MembershipManagerThread;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipNode;

public class ClientMembershipManagerThread extends Thread
{
    private final Random random;
    private final TCPnet net;
    private final DFSManager service;
    private List<GossipNode> members;
    private final ConsistentHasher<GossipMember, String> cHasher;
    
    private AtomicBoolean closed = new AtomicBoolean( false );
    
    private static final Logger LOGGER = Logger.getLogger( ClientMembershipManagerThread.class );
    private static final int TIMER_REQUEST = 10000; // 10 seconds.
    private static final int MAX_POOL_SIZE = 128; // Maximum number of nodes.
    
    
    
    public ClientMembershipManagerThread( TCPnet net,
                                    DFSManager service,
                                    ConsistentHasher<GossipMember, String> cHasher )
    {
        setName( "ClientMembershipManager" );
        
        this.net = net;
        this.service = service;
        this.cHasher = cHasher;
        random = new Random();
        members = new ArrayList<>();
    }
    
    @Override
    public void run()
    {
        LOGGER.info( "ClientMembershipManager Thread launched." );
        
        GossipMember server = null;
        while(!closed.get()) {
            try {
                List<GossipMember> members = cHasher.getAllBuckets();
                // If the list is empty it automatically switches in LoadBalancer mode.
                if(members.isEmpty()) {
                    LOGGER.info( "[CLIENT] The list of nodes is empty. We are now using LoadBalancer nodes." );
                    service.setUseLoadBalancers( true );
                    break;
                }
                
                int index = random.nextInt( members.size() );
                server = members.get( index );
                LOGGER.debug( "[CLIENT] Selected: " + server );
                
                Session session = net.tryConnect( server.getHost(), server.getPort() + MembershipManagerThread.PORT_OFFSET, 2000 );
                if(session == null) {
                    cHasher.removeBucket( server );
                    LOGGER.debug( "[CLIENT] Node: " + server + " is unreachable." );
                    if(cHasher.isEmpty()) {
                        LOGGER.info( "[CLIENT] The list of nodes is empty. We are now using LoadBalancer nodes." );
                        service.setUseLoadBalancers( true );
                        break;
                    }
                }
                else {
                    // Receive the list of nodes.
                    MessageResponse message = session.receiveMessage();
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
        
        LOGGER.info( "ClientMembershipManager Thread closed." );
    }
    
    /**
     * Merges the received list with the owned one.
     * 
     * @param remoteNodes   the remote list
    */
    private void mergeLists( List<GossipNode> remoteNodes )
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
    private void removeNode( GossipMember node )
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
        closed.set( true );
        interrupt();
    }
}
