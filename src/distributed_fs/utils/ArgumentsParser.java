/**
 * @author Stefano Ceccotti
*/

package distributed_fs.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

/**
 * Class used to parse the arguments
 * of a specific node.
*/
public class ArgumentsParser
{
	private static CommandLine cmd = null;
	private static Options options = new Options();
	
	private static final String PORT = "p", ADDRESS = "a",
								NODES = "n", RESOURCES = "r",
								DATABASE = "d", LOAD_BALANCERS = "l", HELP = "h";
	
	public static void parseArgs( final String[] args, final int nodeType ) throws ParseException
	{
		if(nodeType != GossipMember.STORAGE)
			options.addOption( PORT, "port", true, "Set the listening port." );
		if(nodeType != GossipMember.LOAD_BALANCER) {
			options.addOption( RESOURCES, "rloc", true, "Set the location of the resources." );
			options.addOption( DATABASE, "dloc", true, "Set the location of the database." );
		}
		
		// Only a client option.
		if(nodeType != GossipMember.STORAGE && nodeType != GossipMember.LOAD_BALANCER)
		    options.addOption( LOAD_BALANCERS, "ldb", true, "Set the use of the load balancers." );
		
		options.addOption( ADDRESS, "addr", true, "Set the ip address of the node." );
		options.addOption( NODES, "node", true, "Add a new node, where arg is in the format hostname:port:nodeType." );
		options.addOption( HELP, "help", false, "Show help." );
		
		CommandLineParser parser = new DefaultParser();
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
	
	public static boolean getLoadBalancers()
	{
	    if(!cmd.hasOption( LOAD_BALANCERS ))
	        return false;
	    
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
	
	private static void help()
	{
		// This prints out some help
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( " ", options );
	 }
}