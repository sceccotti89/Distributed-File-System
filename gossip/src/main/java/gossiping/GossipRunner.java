package gossiping;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import gossiping.event.GossipListener;

public class GossipRunner 
{
	/** The startup gossiping service. */
	private GossipService gossipService;
	
	public GossipRunner( String configFile, GossipListener listener, String _address, int port, int virtualNodes, int nodeType ) 
	{
		if (configFile != null) {
			try {
				System.out.println( "Parsing the configuration file " + configFile + "..." );
				StartupSettings settings = StartupSettings.fromJSONFile( configFile, _address, port, virtualNodes, nodeType );
				gossipService = new GossipService( settings, listener );
				System.out.println( "Gossip service successfully initialized." );
			} catch ( FileNotFoundException e ) {
				System.err.println("The given file is not found!");
			} catch (JSONException e) {
				System.err.println("The given file is not in the correct JSON format!");
			} catch (IOException e) {
				System.err.println("Could not read the configuration file: " + e.getMessage());
			} catch (InterruptedException e) {
				System.err.println("Error while starting the gossip service: " + e.getMessage());
			}
		} else {
			System.out.println(
				"The " + configFile + " file is not found.\n\n" +
				"Either specify the path to the startup settings file or place the " + configFile +
				" file in the same folder as the JAR file."
			);
		}
	}
	
	public GossipRunner( GossipListener listener, String _address, int port,
						 String id, int virtualNodes, int nodeType )
	{
		this( listener, _address, port, id, virtualNodes, nodeType, new ArrayList<>() );
	}
	
	public GossipRunner( GossipListener listener, String _address, int port,
                         String id, int virtualNodes,
                         int nodeType, List<GossipMember> members )
    {
        try {
            StartupSettings settings = new StartupSettings( _address, port, id, virtualNodes, nodeType, LogLevel.DEBUG, new ArrayList<>( members ) );
            settings.addGossipMember( new RemoteGossipMember( _address, port, id, virtualNodes, nodeType ) );
            gossipService = new GossipService( settings, listener );
            System.out.println( "Gossip service successfully initialized." );
        } catch (InterruptedException e) {
            System.err.println( "Error while starting the gossip service: " + e.getMessage() );
        } catch (UnknownHostException e) {
            System.err.println( "Error while starting the gossip service: " + e.getMessage() );
        }
    }
	
	public GossipService getGossipService()
	{
		return gossipService;
	}
}
