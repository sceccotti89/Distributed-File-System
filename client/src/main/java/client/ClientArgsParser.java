/**
 * @author Stefano Ceccotti
*/

package client;

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
 * of the client.
*/
public class ClientArgsParser
{
    private static CommandLine cmd = null;
    private static Options options;
    
    private static final String PORT = "p", ADDRESS = "a",
                                NODES = "n", RESOURCES = "r",
                                DATABASE = "d", LOCAL_ENV = "locale",
                                FILE = "f", HELP = "h";
    
    public static void parseArgs( String[] args ) throws ParseException
    {
        options = new Options();
        
        options.addOption( FILE, "file", true, "Set the input configuration file." );
        options.addOption( PORT, "port", true, "Set the listening port." );
        options.addOption( RESOURCES, "rloc", true, "Set the location of the resources." );
        options.addOption( DATABASE, "dloc", true, "Set the location of the database." );
        options.addOption( ADDRESS, "addr", true, "Set the ip address of the node." );
        options.addOption( NODES, "node", true, "Add a new node, where arg is in the format hostname:port:nodeType." );
        options.addOption( LOCAL_ENV, false, "Start the system in the local environment." );
        options.addOption( HELP, "help", false, "Show help." );
        
        DefaultParser parser = new DefaultParser();
        cmd = parser.parse( options, args );
        if(cmd.hasOption( HELP ))
            help();
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
            return -1;
        
        return Integer.parseInt( cmd.getOptionValue( PORT ) );
    }
    
    public static List<GossipMember> getNodes() throws ParseException
    {
        if(!cmd.hasOption( NODES )) 
            return null;
        
        String[] nodes = cmd.getOptionValues( NODES );
        return parseNodes( nodes );
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

    private static List<GossipMember> parseNodes( String[] nodes ) throws ParseException
    {
        List<GossipMember> members = new ArrayList<>( nodes.length );
        
        for(String node : nodes) {
            String[] values = node.split( ":" );
            if(values.length != 3) {
                help();
                throw new ParseException( "Invalid number of node attributes.\nThe syntax is hostname:port:nodeType." );
            }
            
            String hostname = values[0];
            int port = Integer.parseInt( values[1] );
            int nodeType = Integer.parseInt( values[2] );
            String id = DFSUtils.getNodeId( 1, hostname );
            
            members.add( new RemoteGossipMember( hostname, port, id, 0, nodeType ) );
        }
        
        return members;
    }
    
    public static boolean isLocalEnv() {
        return cmd.hasOption( LOCAL_ENV );
    }
    
    public static boolean hasOnlyHelpOption() {
        return cmd.hasOption( HELP ) && cmd.getOptions().length == 1;
    }
    
    public static boolean hasOnlyFileOption() {
        return cmd.hasOption( FILE ) && cmd.getOptions().length == 1;
    }
    
    private static void help()
    {
        // This prints out some help
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( " ", options );
     }
}
