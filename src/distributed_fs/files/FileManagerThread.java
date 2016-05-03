/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.merkle_tree.AntiEntropyReceiverThread;
import distributed_fs.merkle_tree.AntiEntropySenderThread;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;

public class FileManagerThread extends Thread
{
	private final ExecutorService threadPoolSend; // Pool used to send the files in parallel.
	private final ExecutorService threadPoolReceive; // Pool used to receive the files in parallel.
	private final DFSDatabase database; // The database where the files are stored.
	
	private StorageNode node; // The associated storage node
	
	private final AntiEntropySenderThread sendAE_t;
	private final AntiEntropyReceiverThread receiveAE_t;
	
	private final TCPnet net;
	
	public static final short DEFAULT_PORT = 7535; // Default port used to send/receive files.
	public static final Logger LOGGER = Logger.getLogger( FileManagerThread.class );
	
	public FileManagerThread( final GossipMember node,
							  final int port,
							  final ConsistentHasherImpl<GossipMember, String> cHasher ) throws IOException
	{
		database = new DFSDatabase( this );
		sendAE_t = new AntiEntropySenderThread( node, database, this, cHasher );
		receiveAE_t = new AntiEntropyReceiverThread( node, database, this, cHasher );
		
		net = new TCPnet( node.getHost(), port );
		
		threadPoolSend = Executors.newCachedThreadPool();
		threadPoolReceive = Executors.newFixedThreadPool( 32 );
		
		LOGGER.setLevel( Utils.logLevel );
		
		LOGGER.info( "File Manager Thread successfully initialized" );
	}
	
	public void addStorageNode( final StorageNode node ) {
		this.node = node;
	}
	
	@Override
	public void run()
	{
		receiveAE_t.start();
		sendAE_t.start();
		
		try {
			LOGGER.info( "File Manager Thread launched" );
			
			while(true) {
				TCPSession session = net.waitForConnection();
				LOGGER.info( "Received a new connection from \"" + session.getSrcAddress() + "\"" );
				threadPoolReceive.execute( new ReceiveFilesThread( session ) );
			}
		}
		catch( IOException e ) {
			//e.printStackTrace();
		}
		
		net.close();
	}
	
	/** 
	 * Saves the incoming files.
	 * 
	 * @param session
	 * @param data
	*/
	private void readFilesToSave( final TCPSession session, ByteBuffer data ) throws IOException, SQLException
	{
		// read the synch attribute
		boolean synch = (data.get() == (byte) 0x1);
		// get and verify the quorum attribute
		if(data.get() == (byte) 0x1)
			node.setBlocked( false );
		// get the number of files
		int num_files = data.getInt();
		LOGGER.debug( "Receiving " + num_files + " files..." );
		
		// read (if present) the hinted handoff address
		String hintedHandoff = null;
		if(data.remaining() > 0)
			hintedHandoff = new String( Utils.getNextBytes( data ) );
		
		for(int i = 0; i < num_files; i++) {
			//data = ByteBuffer.wrap( session.receiveMessage() );
			RemoteFile file = Utils.deserializeObject( session.receiveMessage() );
			LOGGER.debug( "File \"" + file.getName() + "\" downloaded." );
			database.saveFile( file, hintedHandoff, true );
		}
		
		LOGGER.debug( "Files successfully downloaded." );
		
		// just 1 byte (for the synchronization ACK)
		if(synch)
			session.sendMessage( Networking.TRUE, false );
	}
	
	/** 
	 * Deletes all the received files.
	 * 
	 * @param session
	 * @param data
	*/
	private void readFilesToDelete( final TCPSession session, final ByteBuffer data ) throws IOException, SQLException
	{
		// get and verify the quorum attribute
		if(data.get() == (byte) 0x1)
			node.setBlocked( false );
		
		int size = data.getInt();
		for(int i = 0; i < size; i++) {
			DistributedFile file = Utils.deserializeObject( session.receiveMessage() );
			database.removeFile( file.getName(), file.getVersion() );
			LOGGER.debug( "Deleted file \"" + file.getName() + "\"" );
		}
	}
	
	/** 
	 * Sends the list of files, for saving or deleting operation, to the destination address.
	 * 
	 * @param port				destination port
	 * @param opType			operation type (PUT or DELETE)
	 * @param files				list of files
	 * @param address			destination IP address
	 * @param hintedHandoff		the hinted handoff node address
	 * @param wait_response		{@code true} if the process have to wait the response, {@code false} otherwise
	 * @param synchNodeId		identifier of the synchronizing node
	 * @param isQuorum			{@code true} if the message is for the termination of a quorum,
	 * 							{@code false} otherwise
	 * 
	 * @return {@code true} if the files are successfully transmitted, {@code false} otherwise
	*/
	public boolean sendFiles( final int port, final byte opType, final List<DistributedFile> files,
							  final String address, final String hintedHandoff,
							  final boolean wait_response, final byte[] synchNodeId,
							  final boolean isQuorum )
	{
		SendFilesThread t = new SendFilesThread( port, opType, files, address, hintedHandoff, synchNodeId, isQuorum );
		threadPoolSend.execute( t );
		
		if(!wait_response)
			return true;
		
		try{ t.join(); }
		catch( InterruptedException e ){ return false; }
		
		return t.getResult();
	}
	
	/** 
	 * Sends the list of files to the destination address in parallel with the computation.
	 * 
	 * @param port				destination port
	 * @param opType			operation type (PUT or DELETE)
	 * @param files				list of files
	 * @param address			destination IP address
	 * @param hintedHandoff		the hinted handoff node address
	 * @param synchNodeId		identifier of the synchronizing node
	 * @param isQuorum			{@code true} if the message is for the termination of a quorum,
	 * 							{@code false} otherwise
	 * 
	 * @return {@code true} if the files are successfully transmitted,
	 * 		   {@code false} otherwise
	*/
	private boolean transmitFiles( final int port, final byte opType,
								   final List<DistributedFile> files, final String address,
								   final String hintedHandoff, final byte[] synchNodeId,
								   final boolean isQuorum )
	{
		boolean complete = true;
		TCPSession session = null;
		
		LOGGER.debug( "Type " + opType + ": sending " + files.size() + " files to \"" + address + "\"" );
		
		try {
			//session = net.tryConnect( address, DEFAULT_PORT );
			session = net.tryConnect( address, port );
			
			int size = files.size();
			
			byte[] msg;
			if(opType == Message.PUT) {
				msg = net.createMessage( new byte[]{ opType, (synchNodeId != null) ? (byte) 0x1 : (byte) 0x0, (isQuorum) ? (byte) 0x1 : (byte) 0x0 },
										 Utils.intToByteArray( size ),
										 false );
				
				if(hintedHandoff != null)
					msg = net.createMessage( msg, hintedHandoff.getBytes( StandardCharsets.UTF_8 ), true );
			}
			else {
				// DELETE operation
				msg = net.createMessage( new byte[]{ opType, (isQuorum) ? (byte) 0x1 : (byte) 0x0 },
										 Utils.intToByteArray( size ),
										 false );
			}
			
			session.sendMessage( msg, true );
			
			for(int i = 0; i < size; i++) {
				// get the serialization of the file
				byte bytes[];
				DistributedFile dFile = files.get( i );
				if(opType == Message.DELETE)
					bytes = Utils.serializeObject( dFile );
				else{
					RemoteFile file = new RemoteFile( dFile );
					bytes = Utils.serializeObject( file );
				}
				
				LOGGER.debug( "Sending file \"" + dFile.getName() + "\"" );
				session.sendMessage( bytes, true );
				LOGGER.debug( "File \"" + dFile.getName() + "\" transmitted." );
			}
			
			if(synchNodeId != null) {
				session.receiveMessage();
				receiveAE_t.removeFromSynch( synchNodeId );
			}
		}
		catch( IOException e ) {
			//e.printStackTrace();
			complete = false;
			if(synchNodeId != null)
				receiveAE_t.removeFromSynch( synchNodeId );
		}
		
		if(session != null)
			session.close();
		
		return complete;
	}
	
	/**
	 * Returns the own database.
	*/
	public DFSDatabase getDatabase()
	{
		return database;
	}

	/**
	 * Thread used to read incoming files, in an asynchronously way.
	*/
	private class ReceiveFilesThread extends Thread
	{
		private TCPSession session;
		
		public ReceiveFilesThread( final TCPSession session )
		{
			this.session = session;
		}
		
		@Override
		public void run()
		{
			try {
				ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
				
				byte opType = data.get();
				//LOGGER.debug( "Operation: " + ((byte) type) + ", PUT: " + ((char) Utils.PUT) );
				switch( opType ) {
					case( Message.PUT ):
						readFilesToSave( session, data );
						break;
					
					case( Message.DELETE ):
						readFilesToDelete( session, data );
						break;
				}
			}
			catch( IOException | SQLException e ) {
				e.printStackTrace();
			}
			
			session.close();
		}
	}
	
	/**
	 * Thread used to send files, in an asynchronously way.
	*/
	private class SendFilesThread extends Thread
	{
		private int port;
		private byte opType;
		private List<DistributedFile> files;
		private String address;
		private String hinted_handoff;
		private byte[] synchNodeId;
		private boolean isQuorum;
		
		private boolean result;
		
		public SendFilesThread( final int port, final byte opType,
								final List<DistributedFile> files, final String address,
								final String hinted_handoff, final byte[] synchNodeId,
								final boolean isQuorum )
		{
			this.port = port;
			this.opType = opType;
			this.files = files;
			this.address = address;
			this.hinted_handoff = hinted_handoff;
			this.synchNodeId = synchNodeId;
			this.isQuorum = isQuorum;
		}
		
		@Override
		public void run()
		{
			result = transmitFiles( port, opType, files, address, hinted_handoff, synchNodeId, isQuorum );
		}
		
		/** 
		 * Returns the result of the operation.
		*/
		public boolean getResult()
		{
			return result;
		}
	}
}