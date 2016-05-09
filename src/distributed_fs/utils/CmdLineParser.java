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
	
	public static void parseArgs( final String[] args ) throws ParseException
	{
		options.addOption( "h", "help", false, "Show help." );
		options.addOption( "n", "node", true, "Add a new node in the format hostname:port:nodeType." );
		
		CommandLineParser parser = new DefaultParser();
		cmd = parser.parse( options, args );
	}
	
	public static List<GossipMember> getNodes( final String option ) throws ParseException
	{
		if(!cmd.hasOption( option )) {
			//help();
			//throw new ParseException( "Invalid option '-" + option + "'." );
			return null;
		}
		
		String[] nodes = cmd.getOptionValues( option );
		return parseNodes( nodes );
	}
	
	private static List<GossipMember> parseNodes( final String[] nodes ) throws ParseException
	{
		List<GossipMember> members = new ArrayList<>( nodes.length );
		
		for(String node : nodes) {
			//System.out.println( "Node: " + node );
			
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
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp( "Main", options );
	 }
}