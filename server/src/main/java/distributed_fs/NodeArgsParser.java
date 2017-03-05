/**
 * @author Stefano Ceccotti
*/

package distributed_fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONObject;

import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

/**
 * Class used to parse the arguments
 * of a specific node.
*/
public class NodeArgsParser
{
    private static CommandLine cmd = null;
    private static Options options;
    
    private static final String TYPE = "t", PORT = "p", ADDRESS = "a",
                                NODES = "n", V_NODES = "v", RESOURCES = "r",
                                DATABASE = "d", FILE = "f", HELP = "h";
    
    public static void parseArgs( final String[] args ) throws ParseException
    {
        // Look for the node type.
        int nodeType;
        int i, length = args.length;
        for(i = 0; i < length; i++) {
            if(args[i].equals( "-t" ) || args[i].equals( "--type" ))
                break;
        }
        if(i < length)
            nodeType = Integer.parseInt( args[i+1] );
        else {
            throw new ParseException(
                "You must specify the node type.\n" +
                "It must be one of:"                +
                " 0 (for LoadBalancer) or"          +
                " 1 (for StorageNode)."
            );
        }
        
        if(nodeType < 0 || nodeType > 2) {
            throw new ParseException(
                "Invalid node type.\n"      +
                "It must be one of:"        +
                " 0 (for LoadBalancer) or"  +
                " 1 (for StorageNode)."
            );
        }
        
        options = new Options();
        options.addOption( TYPE, "type", true, "Set the node type." );
        
        options.addOption( PORT, "port", true, "Set the gossiping port." );
        if(nodeType == GossipMember.STORAGE) {
            options.addOption( V_NODES, "vnodes", true, "Set the number of virtual nodes." );
            options.addOption( RESOURCES, "rloc", true, "Set the location of the resources." );
            options.addOption( DATABASE, "dloc", true, "Set the location of the database." );
        }
        
        options.addOption( ADDRESS, "addr", true, "Set the ip address of the node." );
        options.addOption( NODES, "node", true, "Add a new node, where arg is in the format hostname:port:nodeType." );
        options.addOption( FILE, "file", true, "Set the input configuration file." );
        
        options.addOption( HELP, "help", false, "Show this helper." );
        
        DefaultParser parser = new DefaultParser();
        cmd = parser.parse( options, args );
        if(cmd.hasOption( HELP ))
            help();
    }
    
    public static Integer getNodeType()
    {
        if(!cmd.hasOption( TYPE ))
            return -1;
        
        return Integer.parseInt( cmd.getOptionValue( TYPE ) );
    }
    
    public static String getIpAddress()
    {
        if(!cmd.hasOption( ADDRESS ))
            return null;
        
        return cmd.getOptionValue( ADDRESS );
    }
    
    public static int getPort()
    {
        if(!cmd.hasOption( PORT ))
            return 0;
        
        return Integer.parseInt( cmd.getOptionValue( PORT ) );
    }
    
    public static List<GossipMember> getNodes() throws ParseException
    {
        if(!cmd.hasOption( NODES )) 
            return null;
        
        String[] nodes = cmd.getOptionValues( NODES );
        return parseNodes( nodes );
    }
    
    public static int getVirtualNodes() throws ParseException
    {
        if(!cmd.hasOption( V_NODES )) 
            return 0;
        
        return Integer.parseInt( cmd.getOptionValue( V_NODES ) );
    }
    
    public static String getResourceLocation()
    {
        if(!cmd.hasOption( RESOURCES ))
            return null;
        
        return cmd.getOptionValue( RESOURCES );
    }
    
    public static String getDatabaseLocation()
    {
        if(!cmd.hasOption( DATABASE ))
            return null;
        
        return cmd.getOptionValue( DATABASE );
    }
    
    public static JSONObject getConfigurationFile() throws IOException
    {
        if(!cmd.hasOption( FILE ))
            return null;
        
        return DFSUtils.parseJSONFile( cmd.getOptionValue( FILE ) );
    }

    private static List<GossipMember> parseNodes( final String[] nodes ) throws ParseException
    {
        List<GossipMember> members = new ArrayList<>( nodes.length );
        
        for(String node : nodes) {
            String[] values = node.split( ":" );
            if(values.length != 3) {
                help();
                throw new ParseException( "Invalid number of node attributes.\n" +
                                          "The syntax is hostname:port:nodeType." );
            }
            
            String hostname = values[0];
            int port = Integer.parseInt( values[1] );
            int nodeType = Integer.parseInt( values[2] );
            String id = DFSUtils.getNodeId( 1, hostname );
            
            members.add( new RemoteGossipMember( hostname, port, id, 0, nodeType ) );
        }
        
        return members;
    }
    
    public static boolean hasOnlyHelpOptions() {
        return cmd.hasOption( HELP ) && cmd.getOptions().length == 2; // +1 for nodeType
    }
    
    public static boolean hasOnlyFileOptions() {
        return cmd.hasOption( FILE ) && cmd.getOptions().length == 2; // +1 for nodeType
    }
    
    private static void help()
    {
        // This prints out some help
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( " ", options );
     }
}
