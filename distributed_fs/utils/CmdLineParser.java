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
public class CmdLineParser
{
	private static CommandLine cmd = null;
	private static Options options = new Options();
	
	private static final String PORT = "p", ADDRESS = "a", NODES = "n", RESOURCES = "r", DATABASE = "d", HELP = "h";
	
	public static void parseArgs( final String[] args, final int nodeType ) throws ParseException
	{
		if(nodeType != GossipMember.STORAGE)
			options.addOption( PORT, "port", true, "Set the listening port." );
		options.addOption( ADDRESS, "addr", true, "Set the ip address of the node." );
		if(nodeType != GossipMember.LOAD_BALANCER) {
			options.addOption( RESOURCES, "rloc", true, "Set the location of the resources." );
			options.addOption( DATABASE, "dloc", true, "Set the location of the database." );
		}
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
			String id = Utils.bytesToHex( Utils.getNodeId( 1, hostname ).array() );
			
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