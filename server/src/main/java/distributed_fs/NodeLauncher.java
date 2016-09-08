/**
 * @author Stefano Ceccotti
*/

package distributed_fs;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.json.JSONObject;

import distributed_fs.exception.DFSException;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import gossiping.GossipMember;

/**
 * Class used to start a remote node.
 * The choose of which node will be started,
 * depends on the type of the parameters.<br>
 * Take a look on the {@link distributed_fs.NodeArgsParser NodeArgsParser}
 * class for further informations.
*/
public class NodeLauncher
{
    public static void main( final String args[] ) throws ParseException, IOException, InterruptedException, DFSException
    {
        NodeArgsParser.parseArgs( args );
        if(NodeArgsParser.hasOnlyHelpOptions())
            return;
        
        JSONObject configFile = NodeArgsParser.getConfigurationFile();
        if(configFile != null && !NodeArgsParser.hasOnlyFileOptions()) {
            throw new ParseException( "If you have defined a configuration file " +
                                      "the other options are not needed." );
        }
        
        switch( NodeArgsParser.getNodeType() ) {
            case( GossipMember.LOAD_BALANCER ):
                String ipAddress = NodeArgsParser.getIpAddress();
                int port = NodeArgsParser.getPort();
                List<GossipMember> members = NodeArgsParser.getNodes();
                
                LoadBalancer balancer = new LoadBalancer( ipAddress, port, members );
                balancer.launch( false );
                break;
                
            case( GossipMember.STORAGE ):
                StorageNode node;
                if(configFile != null)
                    node = StorageNode.fromJSONFile( configFile );
                else {
                    ipAddress = NodeArgsParser.getIpAddress();
                    port = NodeArgsParser.getPort();
                    int vNodes = NodeArgsParser.getVirtualNodes();
                    members = NodeArgsParser.getNodes();
                    String resourceLocation = NodeArgsParser.getResourceLocation();
                    String databaseLocation = NodeArgsParser.getDatabaseLocation();
                    
                    node = new StorageNode( ipAddress, port, vNodes, members, resourceLocation, databaseLocation );
                }
                node.launch( false );
                break;
        }
    }
}
