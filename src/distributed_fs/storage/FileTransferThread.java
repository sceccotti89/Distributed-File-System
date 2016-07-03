/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import distributed_fs.anti_entropy.AntiEntropyReceiverThread;
import distributed_fs.anti_entropy.AntiEntropySenderThread;
import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.overlay.manager.QuorumThread;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

public class FileTransferThread extends Thread
{
	private final ExecutorService threadPoolSend; // Pool used to send the files in parallel.
	private final ExecutorService threadPoolReceive; // Pool used to receive the files in parallel.
	private final DFSDatabase database; // The database where the files are stored.
	
	private QuorumThread quorum_t;
	
	private AntiEntropySenderThread sendAE_t;
	private AntiEntropyReceiverThread receiveAE_t;
	
	private boolean disabledAntiEntropy = false;
	private final ConsistentHasher<GossipMember, String> cHasher;
	private final GossipMember node;
	private boolean shutDown = false;
	
	private final TCPnet net;
	
	private static final int MAX_CONN = 32; // Maximum number of accepted connections.
	public static final Logger LOGGER = Logger.getLogger( FileTransferThread.class );
	
	
	
	public FileTransferThread( final GossipMember node,
							   final int port,
							   final ConsistentHasher<GossipMember, String> cHasher,
							   final QuorumThread quorum_t,
							   final String resourcesLocation,
							   final String databaseLocation ) throws IOException, DFSException
	{
		database = new DFSDatabase( resourcesLocation, databaseLocation, this );
		
		this.node = node;
		this.cHasher = cHasher;
		sendAE_t = new AntiEntropySenderThread( node, database, this, cHasher );
		receiveAE_t = new AntiEntropyReceiverThread( node, database, this, cHasher );
		
		net = new TCPnet( node.getHost(), port );
		
		//threadPoolSend = Executors.newCachedThreadPool();
		threadPoolSend = Executors.newFixedThreadPool( MAX_CONN );
		threadPoolReceive = Executors.newFixedThreadPool( MAX_CONN );
		
		this.quorum_t = quorum_t;
		
		LOGGER.info( "File Manager Thread successfully initialized" );
	}
	
	@Override
	public void run()
	{
	    if(!disabledAntiEntropy) {
    		receiveAE_t.start();
    		sendAE_t.start();
	    }
		
		try {
			LOGGER.info( "File Manager Thread launched" );
			
			while(!shutDown) {
				TCPSession session = net.waitForConnection();
				LOGGER.info( "Received a connection from \"" + session.getSrcAddress() + "\"" );
				synchronized( threadPoolReceive ) {
				    if(threadPoolReceive.isShutdown())
				        break;
				    
				    threadPoolReceive.execute( new ReceiveFilesThread( session ) );
				}
			}
		}
		catch( IOException e ) {
			//e.printStackTrace();
		}
		
		net.close();
		
		LOGGER.info( "Thread Manager Thread closed." );
	}
	
	/**
     * Enable/disable the anti-entropy mechanism.<br>
     * By default this value is setted to {@code true}.
     * 
     * @param enable    {@code true} to enable the anti-entropy mechanism,
     *                  {@code false} otherwise
    */
    public void setAntiEntropy( final boolean enable )
    {
        if(disabledAntiEntropy == !enable)
            return;
        disabledAntiEntropy = !enable;
        
        if(!disabledAntiEntropy) {
            sendAE_t = new AntiEntropySenderThread( node, database, this, cHasher );
            receiveAE_t = new AntiEntropyReceiverThread( node, database, this, cHasher );
        }
        else {
            // Close the background Anti-Entropy Threads.
            if(sendAE_t != null && receiveAE_t != null) {
                sendAE_t.close();
                receiveAE_t.close();
            }
        }
    }
	
	/** 
	 * Reads the incoming files and apply the appropriate operation,
	 * based on file's deleted bit.
	 * 
	 * @param session
	 * @param data
	*/
	private void readFiles( final TCPSession session, ByteBuffer data ) throws IOException, InterruptedException
	{
		// Read the synch attribute.
		boolean synch = (data.get() == (byte) 0x1);
		// Get and verify the quorum attribute.
		if(data.get() == (byte) 0x1) {
			String fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
			quorum_t.setLocked( false, fileName, data.getLong(), (byte) 0x0 ); // Here the operation type is useless.
		}
		
		// get the number of files
		int num_files = data.getInt();
		LOGGER.debug( "Receiving " + num_files + " files..." );
		
		for(int i = 0; i < num_files; i++) {
			data = ByteBuffer.wrap( session.receiveMessage() );
			if(data.get() == Message.PUT) {
				RemoteFile file = new RemoteFile( DFSUtils.getNextBytes( data ) );
				LOGGER.debug( "File \"" + file + "\" downloaded." );
				database.saveFile( file, file.getVersion(), null, true );
			}
			else {
			    // DELETE operation.
				DistributedFile file = DFSUtils.deserializeObject( DFSUtils.getNextBytes( data ) );
				LOGGER.debug( "File \"" + file + "\" downloaded." );
				database.removeFile( file.getName(), file.getVersion(), file.getHintedHandoff() );
				//System.out.println( "ID: " + file.getId() );
			}
		}
		
		LOGGER.debug( "Files successfully downloaded." );
		
		// Send just 1 byte for the synchronization.
		if(synch)
			session.sendMessage( Networking.TRUE, false );
	}
	
	/** 
	 * Sends the list of files, for saving or deleting operation, to the destination address.
	 * 
	 * @param address           destination IP address
	 * @param port				destination port
	 * @param files				list of files
	 * @param wait_response		{@code true} if the process have to wait the response, {@code false} otherwise
	 * @param synchNodeId		identifier of the synchronizing node (used during the anti-entropy phase)
	 * @param node			    
	 * 
	 * @return {@code true} if the files are successfully transmitted, {@code false} otherwise
	*/
	public boolean sendFiles( final String address,
	                          final int port,
							  final List<DistributedFile> files,
							  final boolean wait_response,
							  final String synchNodeId,
							  final QuorumNode node )
	{
		SendFilesThread t = new SendFilesThread( port, files, address, synchNodeId, node );
		synchronized( threadPoolSend ) {
		    if(!threadPoolSend.isShutdown())
		        threadPoolSend.execute( t );
		    else
		        return false;
		}
		
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
	 * @param files				list of files
	 * @param address			destination IP address
	 * @param synchNodeId		identifier of the synchronizing node (used during the anti-entropy phase)
	 * @param node			
	 * 
	 * @return {@code true} if the files are successfully transmitted,
	 * 		   {@code false} otherwise
	*/
	private boolean transmitFiles( final int port,
								   final List<DistributedFile> files,
								   final String address,
								   final String synchNodeId,
								   final QuorumNode node )
	{
		boolean complete = true;
		TCPSession session = null;
		
		try {
			LOGGER.debug( "Connecting to " + address + ":" + port );
			session = net.tryConnect( address, port );
			
			int size = files.size();
			LOGGER.debug( "Sending " + size + " files to \"" + address + ":" + port + "\"" );
			
			byte[] msg = new byte[]{ (synchNodeId != null) ? (byte) 0x1 : (byte) 0x0, (node != null) ? (byte) 0x1 : (byte) 0x0 };
			if(node != null) {
				msg = net.createMessage( msg, files.get( 0 ).getName().getBytes( StandardCharsets.UTF_8 ), true );
				msg = net.createMessage( msg, DFSUtils.longToByteArray( node.getId() ), false );
			}
			msg = net.createMessage( msg, DFSUtils.intToByteArray( size ), false );
			session.sendMessage( msg, true );
			
			for(int i = 0; i < size; i++) {
				DistributedFile dFile = files.get( i );
				if(dFile.isDeleted())
					msg = net.createMessage( new byte[]{ Message.DELETE }, DFSUtils.serializeObject( dFile ), true );
				else {
					RemoteFile file = new RemoteFile( dFile, database.getFileSystemRoot() );
					msg = net.createMessage( new byte[]{ Message.PUT }, file.read(), true );
				}
				
				LOGGER.debug( "Sending file \"" + dFile + "\"" );
				session.sendMessage( msg, true );
				LOGGER.debug( "File \"" + dFile.getName() + "\" transmitted." );
			}
			
			if(synchNodeId != null) {
				session.receiveMessage();
				receiveAE_t.removeFromSynch( synchNodeId );
			}
			
			if(node != null)
				updateQuorum( node );
		}
		catch( IOException e ) {
			//e.printStackTrace();
			complete = false;
			if(synchNodeId != null) {
				receiveAE_t.removeFromSynch( synchNodeId );
				if(node != null)
					updateQuorum( node );
			}
		}
		
		if(session != null)
			session.close();
		
		return complete;
	}
	
	/**
	 * Updates the quorum state.
	 * 
	 * @param node    the quorum node
	*/
	private synchronized void updateQuorum( final QuorumNode node )
	{
		List<QuorumNode> nodes = node.getList();
		nodes.remove( node );
	}
	
	/**
	 * Returns the own database.
	*/
	public DFSDatabase getDatabase()
	{
		return database;
	}
	
	/**
	 * Closes all the opened resources.
	*/
	public void shutDown()
	{
	    shutDown = true;
	    synchronized( threadPoolSend ) {
	        threadPoolSend.shutdown();
	    }
		synchronized( threadPoolReceive ) {
		    threadPoolReceive.shutdown();
		}
		database.close();
		sendAE_t.close();
		receiveAE_t.close();
	}

	/**
	 * Thread used to read incoming files, in an asynchronous way.
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
				readFiles( session, data );
			}
			catch( IOException | InterruptedException e ) {
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
		//private byte opType;
		private List<DistributedFile> files;
		private String address;
		private String synchNodeId;
		private QuorumNode node;
		
		private boolean result;
		
		public SendFilesThread( final int port,
								final List<DistributedFile> files,
								final String address,
								final String synchNodeId,
								final QuorumNode node )
		{
			this.port = port;
			this.files = files;
			this.address = address;
			this.synchNodeId = synchNodeId;
			this.node = node;
		}
		
		@Override
		public void run()
		{
			result = transmitFiles( port, files, address, synchNodeId, node );
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