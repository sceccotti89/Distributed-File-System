package gossiping.manager.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.json.JSONArray;

import gossiping.GossipNode;
import gossiping.GossipService;
import gossiping.LocalGossipMember;
import gossiping.manager.ActiveGossipThread;
import gossiping.manager.GossipManager;

abstract public class SendMembersActiveGossipThread extends ActiveGossipThread 
{
	public SendMembersActiveGossipThread( final GossipManager gossipManager ) 
	{
		super( gossipManager );
	}

	@Override
	protected void sendMembershipList( final LocalGossipMember me, final List<GossipNode> memberList ) 
	{
		GossipService.LOGGER.debug("Send sendMembershipList() is called.");
		me.setHeartbeat(me.getHeartbeat() + 1);
		synchronized (memberList) {
			try {
				LocalGossipMember member = selectPartner(memberList);
				if (member != null) {
					InetAddress dest = InetAddress.getByName( member.getHost() );
					JSONArray jsonArray = new JSONArray();
					GossipService.LOGGER.debug("Sending memberlist to " + dest + ":" + member.getPort());
					jsonArray.put( me.toJSONObject() );
					GossipService.LOGGER.debug( me );
					for (GossipNode other : memberList) {
						jsonArray.put( other.getMember().toJSONObject() );
						GossipService.LOGGER.debug( other.getMember() );
					}
					
					byte[] json_bytes = compressData( jsonArray.toString().getBytes() );
					
					int packet_length = json_bytes.length;
					if (packet_length < GossipManager.MAX_PACKET_SIZE) {
						GossipService.LOGGER.debug( "Sending message (" + packet_length + " bytes): " + jsonArray.toString() );

						ByteBuffer byteBuffer = ByteBuffer.allocate( Integer.BYTES + json_bytes.length );
						byteBuffer.putInt( packet_length );
						byteBuffer.put( json_bytes );
						byte[] buf = byteBuffer.array();

						DatagramSocket socket = new DatagramSocket();
						DatagramPacket datagramPacket = new DatagramPacket( buf, buf.length, dest, member.getPort() );
						socket.send(datagramPacket);
						socket.close();
					} else {
						GossipService.LOGGER.error( "The length of the sending message is too large (" + packet_length + " > " +
													GossipManager.MAX_PACKET_SIZE + ")." );
					}
				}
			} catch ( IOException e1 ) {
				e1.printStackTrace();
			}
		}
	}
	
	/**
	 * Compresses data using a GZIP compressor.
	 * 
	 * @param data	the bytes to compress
	 * 
	 * @return the compressed bytes array
	*/
	private byte[] compressData( final byte[] data )
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try{
			GZIPOutputStream zos = new GZIPOutputStream( baos );
			zos.write( data );
			zos.close();
		}
		catch( IOException e ){
			return null;
		}

		return baos.toByteArray();
	}
}