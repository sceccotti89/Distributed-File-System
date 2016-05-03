/**
 * @author Stefano Ceccotti
*/

package distributed_fs.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_fs.net.messages.Message;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class QuorumSystem
{
	/** Parameters of the quorum protocol (like Dynamo). */
	private static final short N = 3, W = 2, R = 2;
	/** The quorum file status location. */
	public static final String QuorumFile = "./Settings/QuorumStatus.json";
	
	public static void init() throws JSONException, IOException
	{
		if(!Utils.existFile( QuorumFile, true ))
			saveDecision( null );
	}
	
	/**
	 * Load from disk the quorum status.
	 * 
	 * @return the list of nodes to cancel the quorum
	*/
	public static List<GossipMember> loadDecision() throws IOException, JSONException
	{
		List<GossipMember> nodes = new ArrayList<>();
		
		JSONObject file = Utils.parseJSONFile( QuorumFile );
		JSONArray members = file.getJSONArray( "members" );
		for(int i = 0; i < members.length(); i++) {
			JSONObject member = members.getJSONObject( i );
			String hostname = member.getString( "host" );
			int port = member.getInt( "port" );
			nodes.add( new RemoteGossipMember( hostname, port, "", 0, 0 ) );
		}
		
		return nodes;
	}
	
	/**
	 * Save on disk the actual status of the quorum.
	 * 
	 * @param nodes		
	*/
	public static void saveDecision( final List<GossipMember> nodes ) throws IOException, JSONException
	{
		JSONObject file = new JSONObject();
		JSONArray members = new JSONArray();
		
		if(nodes != null && nodes.size() > 0) {
			for(GossipMember node : nodes) {
				JSONObject member = new JSONObject();
				member.put( "host", node.getHost() );
				member.put( "port", node.getPort() );
				members.put( member );
			}
		}
		
		file.put( "members", members );
		
		PrintWriter writer = new PrintWriter( QuorumFile, StandardCharsets.UTF_8.name() );
		writer.println( file.toString() );
		writer.close();
	}
	
	/**
	 * Gets the maximum number of nodes to contact
	 * for the quorum protocol.
	*/
	public static short getMaxNodes() {
		return N;
	}

	public static short getWriters() {
		return W;
	}
	
	public static short getReaders() {
		return R;
	}
	
	public static boolean isReadQuorum( final int readers ) {
		return readers >= R;
	}
	
	public static boolean isWriteQuorum( final int writers ) {
		return writers >= W;
	}
	
	public static boolean isDeleteQuorum( final int deleters ) {
		return deleters >= W;
	}
	
	public static boolean isQuorum( final byte opType, final int replicaNodes ) {
		// TODO TEST (finito il test togliere i commenti)
		//return true;
		if(opType == Message.PUT || opType == Message.DELETE)
			return isWriteQuorum( replicaNodes );
		else
			return isReadQuorum( replicaNodes );
	}
	
	public static boolean unmakeQuorum( final int errors, final byte opType ) {
		if(opType == Message.PUT || opType == Message.DELETE)
			return (N - errors) < W;
		else
			return (N - errors) < R;
	}
	
	public static int getMinQuorum( final byte opType )
	{
		// TODO TEST (finito il test togliere i commenti)
		//return 0;
		
		if(opType == Message.GET)
			return R;
		else
			return W;
	}
	
	public static class QuorumSession
	{
		public QuorumSession()
		{
			
		}
	}
}