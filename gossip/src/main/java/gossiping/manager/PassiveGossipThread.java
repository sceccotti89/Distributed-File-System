package gossiping.manager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import gossiping.GossipMember;
import gossiping.GossipService;
import gossiping.LocalGossipMember;
import gossiping.RemoteGossipMember;

/**
 * [The passive thread: reply to incoming gossip request.] This class handles
 * the passive cycle, where this client has received an incoming message. For
 * now, this message is always the membership list, but if you choose to gossip
 * additional information, you will need some logic to determine the incoming
 * message.
 */
abstract public class PassiveGossipThread implements Runnable 
{
	public static final Logger LOGGER = Logger.getLogger(PassiveGossipThread.class);

	/** The socket used for the passive thread of the gossip service. */
	private DatagramSocket udpServer;
	private final GossipManager gossipManager;
	private AtomicBoolean keepRunning;

	public PassiveGossipThread(final GossipManager gossipManager) 
	{
		this.gossipManager = gossipManager;
		try{
			LocalGossipMember my_self = gossipManager.getMyself();
			SocketAddress socketAddress = new InetSocketAddress( my_self.getHost(), my_self.getPort() );
			//SocketAddress socketAddress = new InetSocketAddress( my_self.getHost(), GossipManager.GOSSIPING_PORT );
			udpServer = new DatagramSocket( socketAddress );
			GossipService.LOGGER.info( "Gossip service successfully initialized on port " + my_self.getPort() );
			GossipService.LOGGER.debug( "I am " + my_self );
		}
		catch( SocketException ex ){
			GossipService.LOGGER.error( ex );
			udpServer = null;
			throw new RuntimeException( ex );
		}

		keepRunning = new AtomicBoolean( true );
	}

	@Override
	public void run() 
	{
		while(keepRunning.get()){
			try {
				byte[] buf = new byte[udpServer.getReceiveBufferSize()];
				DatagramPacket p = new DatagramPacket( buf, buf.length );
				udpServer.receive( p );
				GossipService.LOGGER.debug( "A message has been received from " + p.getAddress() + ":" + p.getPort() + "." );
				
				ByteBuffer buffer = ByteBuffer.wrap( buf );
				int packet_length = buffer.getInt();
				
				// Check whether the package is smaller than the maximal packet length.
				// A package larger than this would not be possible to be send from a GossipService,
				// since this is checked before sending the message.
				// This could normally only occur when the list of members is very big,
				// or when the packet is malformed, and the first 4 bytes is not the right in anymore.
				// For this reason we regards the message.
				if(packet_length <= GossipManager.MAX_PACKET_SIZE) {
					byte[] json_bytes = new byte[packet_length];
					buffer.get( json_bytes );
					json_bytes = decompressData( json_bytes );
					
					String receivedMessage = new String( json_bytes );
					GossipService.LOGGER.debug( "Received message (" + packet_length + " bytes): " + receivedMessage );
					try {
						List<GossipMember> remoteGossipMembers = new ArrayList<>();
						RemoteGossipMember senderMember = null;
						GossipService.LOGGER.debug( "Received member list:" );
						JSONArray jsonArray = new JSONArray( receivedMessage );
						for(int i = 0; i < jsonArray.length(); i++) {
							JSONObject memberJSONObject = jsonArray.getJSONObject( i );
							if (memberJSONObject.length() == 6) {
								RemoteGossipMember member = new RemoteGossipMember(
													memberJSONObject.getString( GossipMember.JSON_HOST ),
													memberJSONObject.getInt( GossipMember.JSON_PORT ),
													memberJSONObject.getString( GossipMember.JSON_ID ),
													memberJSONObject.getInt( GossipMember.JSON_VNODES ),
													memberJSONObject.getInt( GossipMember.JSON_NODE_TYPE ),
													memberJSONObject.getInt( GossipMember.JSON_HEARTBEAT ) );
								GossipService.LOGGER.debug( member.toString() );
								// This is the first member found, so this should be the member who is communicating with me.
								if(i == 0)
									senderMember = member;
									
								remoteGossipMembers.add( member );
							} else {
								GossipService.LOGGER.error( "The received member object does not contain 6 fields:\n" + memberJSONObject.toString() );
							}
						}
						mergeLists( gossipManager, senderMember, remoteGossipMembers );
					} catch ( JSONException e ) {
					GossipService.LOGGER.error( "The received message is not well-formed JSON. The following message has been dropped:\n" + receivedMessage );
					}
				} else {
					GossipService.LOGGER.error( "The received message is not of the expected size, it has been dropped." );
				}
			} catch ( IOException e ) {
				//GossipService.LOGGER.error( e );
			    //e.printStackTrace();
				keepRunning.set( false );
			}
		}
		shutdown();
	}

	/**
	 * Decompresses data using a GZIP decompressor.
	 * This method works only if the input data has been already compressed.
	 * 
	 * @param data	the bytes to decompress
	 * 
	 * @return the decompressed bytes array
	*/
	private byte[] decompressData( byte[] data )
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayInputStream bais = new ByteArrayInputStream( data );

		try{
			GZIPInputStream zis = new GZIPInputStream( bais );
			byte[] tmpBuffer = new byte[256];
			int n;
			while((n = zis.read( tmpBuffer )) >= 0)
				baos.write( tmpBuffer, 0, n );
			zis.close();
		}
		catch( IOException e ){ return null; }

		return baos.toByteArray();
	}

	public void shutdown() 
	{
		udpServer.close();
	}

	/**
	 * Abstract method for merging the local and remote list.
	 *
	 * @param gossipManager
	 *          The GossipManager for retrieving the local members and dead members list.
	 * @param senderMember
	 *          The member who is sending this list, this could be used to send a response if the
	 *          remote list contains out-dated information.
	 * @param remoteList
	 *          The list of members known at the remote side.
	*/
	abstract protected void mergeLists( GossipManager gossipManager,
	                                    RemoteGossipMember senderMember,
										List<GossipMember> remoteList );
}
