/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
								NODES = "n", RESOURCES = "r",
								DATABASE = "d", LOAD_BALANCERS = "l",
								HELP = "h";
	
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
	    
		if(nodeType != GossipMember.STORAGE)
		    options.addOption( PORT, "port", true, "Set the listening port." );
		if(nodeType != GossipMember.LOAD_BALANCER) {
    		options.addOption( RESOURCES, "rloc", true, "Set the location of the resources." );
    		options.addOption( DATABASE, "dloc", true, "Set the location of the database." );
		}
		
		options.addOption( ADDRESS, "addr", true, "Set the ip address of the node." );
		options.addOption( NODES, "node", true, "Add a new node, where arg is in the format hostname:port:nodeType." );
		options.addOption( HELP, "help", false, "Show help." );
		
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
			return -1;
		
		return Integer.parseInt( cmd.getOptionValue( PORT ) );
	}
	
	public static boolean useLoadBalancers()
	{
	    if(!cmd.hasOption( LOAD_BALANCERS ))
	        return true;
	    
	    return Boolean.parseBoolean( cmd.getOptionValue( LOAD_BALANCERS ) );
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

	private static List<GossipMember> parseNodes( final String[] nodes ) throws ParseException
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
	
	public static boolean hasOnlyHelpOptions() {
	    return cmd.hasOption( HELP ) && cmd.getOptions().length == 1;
	}
	
	private static void help()
	{
		// This prints out some help
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( " ", options );
	 }
}