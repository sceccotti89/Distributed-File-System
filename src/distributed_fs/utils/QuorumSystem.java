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
import distributed_fs.overlay.StorageNode.QuorumNode;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class QuorumSystem
{
	public static long timeElapsed;
	
	/** Parameters of the quorum protocol (like Dynamo). */
	private static final short N = 3, W = 2, R = 2;
	/** The quorum file status location. */
	public static final String QuorumFile = "./Settings/QuorumStatus.json";
	
	public static QuorumSession getQuorum( final int id ) throws IOException, JSONException
	{
	    return new QuorumSession( id );
	}
	
	// TODO se uso il QuorumSession dovro' rimuovere init, loadState e saveState
	public static void init() throws JSONException, IOException
	{
		if(!Utils.existFile( QuorumFile, true ))
			saveState( null );
	}
	
	/**
	 * Load from disk the quorum status.
	 * 
	 * @return the list of nodes to cancel the quorum
	*/
	public static List<QuorumNode> loadState() throws IOException, JSONException
	{
		List<QuorumNode> nodes = new ArrayList<>();
		
		JSONObject file = Utils.parseJSONFile( QuorumFile );
		
		long timestamp = file.getLong( "timestamp" );
		timeElapsed = System.currentTimeMillis() - timestamp;
		
		JSONArray members = file.getJSONArray( "members" );
		for(int i = 0; i < members.length(); i++) {
			JSONObject member = members.getJSONObject( i );
			String hostname = member.getString( "host" );
			int port = member.getInt( "port" );
			String fileName = member.getString( "file" );
			byte opType = (byte) member.getInt( "opType" );
			long id = member.getLong( "id" );
			nodes.add( new QuorumNode( new RemoteGossipMember( hostname, port, "", 0, 0 ), fileName, opType, id ) );
		}
		
		return nodes;
	}
	
	/**
	 * Save on disk the actual status of the quorum.
	 * 
	 * @param nodes		list of nodes to be contacted
	*/
	public static void saveState( final List<QuorumNode> nodes ) throws IOException, JSONException
	{
		JSONObject file = new JSONObject();
		
		JSONArray members = new JSONArray();
		if(nodes != null && nodes.size() > 0) {
			for(int i = 0; i < nodes.size(); i++) {
				GossipMember node = nodes.get( i ).getNode();
				JSONObject member = new JSONObject();
				member.put( "host", node.getHost() );
				member.put( "port", node.getPort() );
				member.put( "file" , nodes.get( i ).getFileName() );
				member.put( "opType", nodes.get( i ).getOpType() );
				member.put( "id", nodes.get( i ).getId() );
				members.put( member );
			}
		}
		
		file.put( "members", members );
		file.put( "timestamp", System.currentTimeMillis() );
		
		PrintWriter writer = new PrintWriter( QuorumFile, StandardCharsets.UTF_8.name() );
		writer.println( file.toString() );
		writer.flush();
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
		// TODO TEST (finiti i test togliere i commenti)
		return true;
		/*if(opType == Message.PUT || opType == Message.DELETE)
			return isWriteQuorum( replicaNodes );
		else
			return isReadQuorum( replicaNodes );*/
	}
	
	public static boolean unmakeQuorum( final int errors, final byte opType ) {
		if(opType == Message.PUT || opType == Message.DELETE)
			return (N - errors) < W;
		else
			return (N - errors) < R;
	}
	
	public static int getMinQuorum( final byte opType )
	{
		// TODO TEST (finiti i test togliere i commenti)
		return 0;
		
		/*if(opType == Message.GET)
			return R;
		else
			return W;*/
	}
	
	public static class QuorumSession
	{
	    private final String quorumFile;
	    private long timeElapsed;
	    
		public QuorumSession( final int id ) throws IOException, JSONException
		{
		    quorumFile = "./Settings/QuorumStatus" + id + ".json";
	        if(!Utils.existFile( quorumFile, true ))
	            saveState( null );
		}
		
		/**
	     * Load from disk the quorum status.
	     * 
	     * @return the list of nodes to cancel the quorum
	    */
	    public List<QuorumNode> loadState() throws IOException, JSONException
	    {
	        List<QuorumNode> nodes = new ArrayList<>();
	        
	        JSONObject file = Utils.parseJSONFile( quorumFile );
	        
	        long timestamp = file.getLong( "timestamp" );
	        timeElapsed = System.currentTimeMillis() - timestamp;
	        
	        JSONArray members = file.getJSONArray( "members" );
	        for(int i = 0; i < members.length(); i++) {
	            JSONObject member = members.getJSONObject( i );
	            String hostname = member.getString( "host" );
	            int port = member.getInt( "port" );
	            String fileName = member.getString( "file" );
	            byte opType = (byte) member.getInt( "opType" );
	            long id = member.getLong( "id" );
	            nodes.add( new QuorumNode( new RemoteGossipMember( hostname, port, "", 0, 0 ), fileName, opType, id ) );
	        }
	        
	        return nodes;
	    }
	    
	    /**
	     * Save on disk the actual status of the quorum.
	     * 
	     * @param nodes     list of nodes to be contacted
	    */
	    public void saveState( final List<QuorumNode> nodes ) throws IOException, JSONException
	    {
	        JSONObject file = new JSONObject();
	        
	        JSONArray members = new JSONArray();
	        if(nodes != null && nodes.size() > 0) {
	            for(int i = 0; i < nodes.size(); i++) {
	                GossipMember node = nodes.get( i ).getNode();
	                JSONObject member = new JSONObject();
	                member.put( "host", node.getHost() );
	                member.put( "port", node.getPort() );
	                member.put( "file" , nodes.get( i ).getFileName() );
	                member.put( "opType", nodes.get( i ).getOpType() );
	                member.put( "id", nodes.get( i ).getId() );
	                members.put( member );
	            }
	        }
	        
	        file.put( "members", members );
	        file.put( "timestamp", System.currentTimeMillis() );
	        
	        PrintWriter writer = new PrintWriter( quorumFile, StandardCharsets.UTF_8.name() );
	        writer.println( file.toString() );
	        writer.flush();
	        writer.close();
	    }
	    
	    public long getTimeElapsed() {
	        return timeElapsed;
	    }
	}
}