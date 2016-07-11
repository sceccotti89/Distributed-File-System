
package client.manager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import distributed_fs.exception.DFSException;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class SystemSimulation implements Closeable
{
    private final List<GossipMember> members;
    private List<DFSNode> nodes;
    
    private static final int NUMBER_OF_BALANCERS = 2;
    private static final int NUMBER_OF_STORAGES = 5;
    
    private static final String IpAddress = "127.0.0.1";
    
    public SystemSimulation( final String ipAddress ) throws IOException, DFSException, InterruptedException
    {
        this( ipAddress, DFSUtils.computeVirtualNodes() );
    }
    
    public SystemSimulation( final String ipAddress,
                             final int virtualNodes ) throws IOException, DFSException, InterruptedException
    {
        this( ipAddress, virtualNodes, null );
    }
    
    public SystemSimulation( final String ipAddress,
                             final int virtualNodes,
                             final List<GossipMember> members ) throws IOException, DFSException, InterruptedException
    {
        //DFSUtils.testing = true;
        String address = (ipAddress == null) ? IpAddress : ipAddress;
        
        if(members != null)
            this.members = members;
        else {
            // Create the gossip members and put them in a list.
            this.members = new ArrayList<>( NUMBER_OF_BALANCERS + NUMBER_OF_STORAGES );
            
            int k = 100;
            for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
                int port = 8000 + (i * k);
                String id = DFSUtils.getNodeId( 1, IpAddress + ":" + port );
                //System.out.println( "ID: " + id );
                this.members.add( new RemoteGossipMember( address, port, id, virtualNodes, GossipMember.LOAD_BALANCER ) );
            }
            
            for(int i = 0 ; i < NUMBER_OF_STORAGES; i++, k++) {
                int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
                //System.out.println( "[" + i + "] = " + port );
                String id = DFSUtils.getNodeId( 1, address + ":" + port );
                //System.out.println( "ID: " + id );
                this.members.add( new RemoteGossipMember( address, port, id, virtualNodes, GossipMember.STORAGE ) );
            }
        }
        
        createNodes( address, virtualNodes );
    }
    
    private void createNodes( final String IpAddress, final int vNodes ) throws IOException, DFSException, InterruptedException
    {
        nodes = new ArrayList<>( members.size() );
        
        // Start the load balancer nodes.
        for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
            LoadBalancer node = new LoadBalancer( IpAddress, members.get( i ).getPort(), members );
            node.setGossipingMechanism( false );
            nodes.add( node );
            node.launch( true );
        }
        
        String resources = "./Servers/Resources";
        String database =  "./Servers/Database";
        // Start the storage nodes.
        for(int i = 0; i < NUMBER_OF_STORAGES; i++) {
            GossipMember member = members.get( i + NUMBER_OF_BALANCERS );
            StorageNode node = new StorageNode( IpAddress, member.getPort(), vNodes, members,
                                                resources + (i+2) + "/", database + (i+2) + "/" );
            node.setAntiEntropy( false );
            node.setGossipingMechanism( false );
            nodes.add( node );
            node.launch( true );
        }
    }
    
    /**
     * Returns the list of nodes.
    */
    public List<GossipMember> getNodes()
    {
        return members;
    }
    
    @Override
    public void close()
    {
        if(nodes != null) {
            for(DFSNode node : nodes)
                node.close();
            for(DFSNode node : nodes) {
                try { node.join(); }
                catch( InterruptedException e ) {}
            }
        }
    }
}
