/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_fs.net.messages.Message;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class QuorumSession
{
	private long timeElapsed;
	private String quorumFile;
	
	/** Parameters of the quorum protocol (like Dynamo). */
	private static final short N = 3, W = 2, R = 2;
	/** The location of the quorum file. */
	private static final String QUORUM_LOCATION = "QuorumSessions/";
	/** The quorum file status location. */
	public static final String QUORUM_FILE = "QuorumStatus_";
	
	/**
	 * Construct a new quorum session.
	 * 
	 * @param fileLocation     specify the location of the quorum files. If {@code null} the default location will be used
	 * @param id               identifier used to reference in a unique way the associated quorum file
	*/
	public QuorumSession( final String fileLocation, final long id ) throws IOException, JSONException
	{
	    if(fileLocation == null)
	        quorumFile = QUORUM_LOCATION + "QuorumSession_" + id + ".json";
	    else
	        quorumFile = fileLocation + "QuorumSession_" + id + ".json";
	    System.out.println( "QUORUM FILE: " + quorumFile );
        if(!DFSUtils.existFile( quorumFile, true ))
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
		
		JSONObject file = DFSUtils.parseJSONFile( quorumFile );
		
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
			nodes.add( new QuorumNode( this, new RemoteGossipMember( hostname, port, "", 0, 0 ), fileName, opType, id ) );
		}
		
		return nodes;
	}
	
	/**
	 * Save on disk the actual status of the quorum.
	 * 
	 * @param nodes		list of nodes to be contacted
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
	
	// TODO implementare dall'esterno questa funzione
	public long getTimeElapsed() {
		return timeElapsed;
	}
	
	/**
	 * Close the quorum session.
	*/
	public void closeQuorum()
	{
		if(quorumFile != null)
			new File( quorumFile ).delete();
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
	
	public static int getMinQuorum( final byte opType ) {
		if(opType == Message.GET)
			return R;
		else
			return W;
	}
}