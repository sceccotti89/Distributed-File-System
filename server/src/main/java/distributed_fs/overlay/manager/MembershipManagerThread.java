
package distributed_fs.overlay.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.GossipNode;
import gossiping.manager.GossipManager;

public class MembershipManagerThread extends Thread
{
    private final String address;
    private final int port;
    
    private GossipMember me;
    
    private GossipManager manager;
    private List<GossipNode> members;
    
    private boolean closed = false;
    
    /**
     * Constructor used when the list of members may vary,
     * according to the gossiping protocol.
    */
    public MembershipManagerThread( final String address, final int port,
                                    final GossipMember me, final GossipManager manager )
    {
        this.address = address;
        this.port = port + 4;
        
        this.me = me;
        this.manager = manager;
    }
    
    /**
     * Constructor used when the list of members is fixed.
    */
    public MembershipManagerThread( final String address, final int port,
                                    final List<GossipMember> members )
    {
        this.address = address;
        this.port = port + 4;
        
        this.members = new ArrayList<>( members.size() );
        for(GossipMember member : members)
            this.members.add( new GossipNode( member ) );
    }
    
    @Override
    public void run()
    {
        TCPnet net = new TCPnet();
        net.setSoTimeout( 500 );
        
        while(!closed) {
            try {
                TCPSession session = net.waitForConnection( address, port );
                if(session == null)
                    continue;
                
                // Get the list of members and send it to the user.
                List<GossipNode> members;
                if(manager == null)
                    members = this.members;
                else {
                    List<GossipNode> localMembers = manager.getMemberList();
                    members = new ArrayList<>( localMembers.size() + 1 );
                    members.addAll( localMembers );
                    members.add( new GossipNode( me ) );
                }
                
                MessageResponse message = new MessageResponse();
                for(GossipNode member : members)
                    message.addObject( DFSUtils.serializeObject( member ) );
                session.sendMessage( message, true );
                
                session.close();
            }
            catch( IOException e ) {
                e.printStackTrace();
                break;
            }
        }
        
        net.close();
    }
    
    public void close()
    {
        closed = true;
    }
}