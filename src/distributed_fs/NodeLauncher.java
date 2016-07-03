
package distributed_fs;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;

import distributed_fs.exception.DFSException;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.NodeArgsParser;
import distributed_fs.overlay.StorageNode;
import gossiping.GossipMember;

/**
 * Class used to launch a remote node.
 * The choose of which node will be started,
 * depends on the type of the parameters.<br>
 * Take a look on the {@link distributed_fs.overlay.NodeArgsParser NodeArgsParser}
 * class for further informations.
*/
public class NodeLauncher
{
    public static void main( final String args[] ) throws ParseException, IOException, InterruptedException, DFSException
    {
        NodeArgsParser.parseArgs( args );
        if(NodeArgsParser.hasOnlyHelpOptions())
            return;
        
        switch( NodeArgsParser.getNodeType() )
        {
            case( GossipMember.LOAD_BALANCER ):
                String ipAddress = NodeArgsParser.getIpAddress();
                int port = NodeArgsParser.getPort();
                List<GossipMember> members = NodeArgsParser.getNodes();
                
                LoadBalancer balancer = new LoadBalancer( ipAddress, port, members );
                balancer.launch( true );
                break;
                
            case( GossipMember.STORAGE ):
                ipAddress = NodeArgsParser.getIpAddress();
                members = NodeArgsParser.getNodes();
                String resourceLocation = NodeArgsParser.getResourceLocation();
                String databaseLocation = NodeArgsParser.getDatabaseLocation();
                
                StorageNode node = new StorageNode( ipAddress, members, resourceLocation, databaseLocation );
                node.launch( true );
                break;
        }
    }
}