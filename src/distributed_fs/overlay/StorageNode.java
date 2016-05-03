/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.NotBoundException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.swing.Timer;

import org.json.JSONException;
import org.junit.Test;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.files.DistributedFile;
import distributed_fs.files.FileManagerThread;
import distributed_fs.files.RemoteFile;
import distributed_fs.net.NetworkMonitorSenderThread;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClockInconsistencyResolver;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;
import gossiping.LocalGossipMember;
import gossiping.RemoteGossipMember;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public class StorageNode extends DFSnode
{
	private int port;
	private static LocalGossipMember me;
	private QuorumThread quorum_t;
	
	// ===== Used by the private constructor ===== //
	private TCPSession session;
	private String destId; // Destination node identifier, for an input request.
	private List<GossipMember> agreedNodes; // List of nodes that have agreed to the quorum.
	// =========================================== //
	
	public StorageNode() throws IOException, JSONException, NoSuchAlgorithmException, NotBoundException, SQLException
	{
		super( GossipMember.STORAGE );
		
		me = runner.getGossipService().getGossipManager().getMyself();
		cHasher.addBucket( me, me.getVirtualNodes() );
		
		fMgr = new FileManagerThread( me, FileManagerThread.DEFAULT_PORT, cHasher );
		fMgr.addStorageNode( this );
		fMgr.start();
		
		monitor = new NetworkMonitorSenderThread( _address, this );
		monitor.start();
		
		threadPool.execute( quorum_t = new QuorumThread( port ) );
		
		this.port = me.getPort();
		//this.port = Utils.SERVICE_PORT;
		//this.port = port;
		_net.setSoTimeout( 2000 );
		while(!GossipManager.doShutdown) {
			//System.out.println( "[SN] Waiting on: " + _address + ":" + this.port );
			TCPSession session = _net.waitForConnection( _address, this.port );
			if(session != null)
				threadPool.execute( new StorageNode( fMgr, quorum_t, cHasher, _net, session ) );
		}
		
		closeResources();
	}
	
	/** Testing. */
	public StorageNode( final List<GossipMember> startupMembers,
						final String id,
						final String address,
						final int port ) throws IOException, JSONException
	{
		super();
		
		_address = address;
		
		for(GossipMember member: startupMembers)
			gossipEvent( member, GossipState.UP );
		
		fMgr = new FileManagerThread( new RemoteGossipMember( _address, port, id, 3, GossipMember.STORAGE ), port + 1, cHasher );
		fMgr.addStorageNode( this );
		monitor = new NetworkMonitorSenderThread( _address, this );
		
		this.port = port;
	}
	
	@Test
	public void launch() throws IOException, JSONException, SQLException
	{
		fMgr.start();
		
		monitor.start();
		threadPool.execute( quorum_t = new QuorumThread( port ) );
		
		_net.setSoTimeout( 2000 );
		while(!GossipManager.doShutdown) {
			//System.out.println( "[SN] Waiting on: " + _address + ":" + port );
			TCPSession session = _net.waitForConnection( _address, port );
			if(session != null)
				threadPool.execute( new StorageNode( fMgr, quorum_t, cHasher, _net, session ) );
		}
		
		closeResources();
	}
	
	/**
	 * Constructor used to handle the incoming request.
	 * 
	 * @param fMgr		the file manager thread
	 * @param quorum_t	the quorum thread
	 * @param cHasher	the consistent hashing structure
	 * @param net		the current TCP channel
	 * @param session	the TCP session
	*/
	private StorageNode( final FileManagerThread fMgr,
						 final QuorumThread quorum_t,
						 final ConsistentHasherImpl<GossipMember, String> cHasher,
						 final TCPnet net,
						 final TCPSession session ) throws IOException, JSONException
	{
		super( net, fMgr, cHasher );
		
		this.quorum_t = quorum_t;
		this.session = session;
	}
	
	@Override
	public void run()
	{
		LOGGER.info( "[SN] Received a connection from: " + session.getSrcAddress() );
		stats.increaseValue( NodeStatistics.NUM_CONNECTIONS );
		
		try {
			MessageRequest data = Utils.deserializeObject( session.receiveMessage() );
			byte opType = data.getType();
			boolean isCoordinator = data.startQuorum();
			LOGGER.debug( "[SN] Received: " + opType + ":" + isCoordinator );
			
			if(!isCoordinator) // GET or GET_ALL request
				setBlocked( false );
			else {
				// the connection with the client must be estabilished before the quorum
				openClientConnection( data.getClientAddress() );
				
				// get the destination id, since it can be a virtual node
				destId = data.getDestId();
				
				LOGGER.info( "[SN] Start the quorum..." );
				agreedNodes = quorum_t.checkQuorum( session, opType, destId );
				int replicaNodes = agreedNodes.size();
				
				// checks if the quorum has been completed successfully
				if(!QuorumSystem.isQuorum( opType, replicaNodes )) {
					LOGGER.info( "[SN] Quorum failed: " + replicaNodes + "/" + QuorumSystem.getMinQuorum( opType ) );
					session.close();
					stats.decreaseValue( NodeStatistics.NUM_CONNECTIONS );
					return;
				}
				else {
					if(opType != Message.GET) {
						LOGGER.info( "[SN] Quorum completed succesfully: " + replicaNodes + "/" + QuorumSystem.getMinQuorum( opType ) );
						quorum_t.sendQuorumResponse( session, Message.TRANSACTION_OK );
						QuorumSystem.saveDecision( agreedNodes );
					}
				}
			}
			
			switch( opType ) {
				case( Message.PUT ):
					RemoteFile file = Utils.deserializeObject( data.getFile() );
					
					// get (if present) the hinted handoff address
					String hintedHandoff = data.getHintedHandoff();
					
					LOGGER.debug( "PUT: " + file.getName() + ":" + hintedHandoff );
					
					handlePUT( isCoordinator, file, hintedHandoff );
					break;
					
				case( Message.GET ):
					handleGET( isCoordinator, data.getFileName() );
					break;
					
				case( Message.GET_ALL ):
					handleGET_ALL( data.getClientAddress() );
					break;
				
				case( Message.DELETE ):
					handleDELETE( isCoordinator, Utils.deserializeObject( data.getFile() ) );
					break;
			}
		}
		catch( IOException | SQLException | JSONException e ) {
			e.printStackTrace();
		}
		
		session.close();
		stats.decreaseValue( NodeStatistics.NUM_CONNECTIONS );
	}
	
	private void handlePUT( final boolean isCoordinator, final RemoteFile file, final String hintedHandoff ) throws IOException, SQLException
	{
		file.incrementVersion( _address );
		boolean updated = fMgr.getDatabase().saveFile( file, hintedHandoff, true );
		
		if(updated) {
			// send, in parallel, the file to the replica nodes
			List<DistributedFile> files = Collections.singletonList( new DistributedFile( file ) );
			for(GossipMember node : agreedNodes)
				fMgr.sendFiles( node.getPort() + 1, Message.PUT, files, node.getHost(), hintedHandoff, false, null, true );
		}
	}
	
	private void handleGET( final boolean isCoordinator, final String fileName ) throws IOException
	{
		if(isCoordinator) {
			// send the GET request to all the agreed nodes,
			// to retrieve their version of the file and make the reconciliation
			List<TCPSession> openSessions = sendRequestToReplicaNodes( fileName );
			
			// the replica files can be less than the quorum
			LOGGER.info( "Receive the files from the replica nodes..." );
			HashMap<RemoteFile, byte[]> filesToSend = new HashMap<>( QuorumSystem.getMaxNodes() + 1 );
			int errors = 0;
			
			for(TCPSession session : openSessions) {
				try{
					ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
					if(data.get() == (byte) 0x1) { // replica node own the requested file
						byte[] file = Utils.getNextBytes( data );
						filesToSend.put( Utils.deserializeObject( file ), file );
					}
				} catch( IOException e ) {
					if(QuorumSystem.unmakeQuorum( ++errors, Message.GET )) {
						LOGGER.info( "[SN] Quorum failed: " + openSessions.size() + "/" + QuorumSystem.getMinQuorum( Message.GET ) );
						
						quorum_t.cancelQuorum( this.session, agreedNodes );
						return;
					}
				}
				
				session.close();
			}
			
			// send the positive notification to the client
			LOGGER.info( "[SN] Quorum completed successfully: " + openSessions.size() + "/" + QuorumSystem.getMinQuorum( Message.GET ) );
			MessageResponse message = new MessageResponse( Message.TRANSACTION_OK );
			session.sendMessage( message, true );
			
			// put in the list the file present in the database of this node
			DistributedFile dFile = fMgr.getDatabase().getFile( Utils.getId( fileName ) );
			if(dFile != null) {
				RemoteFile rFile = new RemoteFile( dFile );
				byte[] file = Utils.serializeObject( rFile );
				filesToSend.put( rFile, file );
			}
			
			// try a first reconciliation
			LOGGER.debug( "Files: " + filesToSend.size() );
			List<RemoteFile> reconciledFiles = makeReconciliation( filesToSend );
			LOGGER.debug( "Files after reconciliation: " + reconciledFiles.size() );
			
			// send the files directly to the client
			message = new MessageResponse();
			for(int i = 0; i < reconciledFiles.size(); i++) {
				byte[] data = filesToSend.get( reconciledFiles.get( i ) );
				message.addFile( data );
			}
			session.sendMessage( message, true );
			
			LOGGER.info( "Files sent to the client." );
		}
		else {
			// REPLICA node: send the requested file to the coordinator
			DistributedFile file = fMgr.getDatabase().getFile( Utils.getId( fileName ) );
			if(file == null)
				session.sendMessage( new MessageResponse( (byte) 0x0 ), false );
			else {
				MessageResponse message = new MessageResponse( (byte) 0x1 );
				message.addFile( Utils.serializeObject( new RemoteFile( file ) ) );
				session.sendMessage( message, true );
			}
		}
	}
	
	/**
	 * Make the reconciliation among different vector clocks.
	 * 
	 * @param files		list of files to compare
	 * 
	 * @return The list of uncorrelated versions.
	*/
	private List<RemoteFile> makeReconciliation( final HashMap<RemoteFile, byte[]> files )
	{
		List<Versioned<RemoteFile>> versions = new ArrayList<>();
		for(RemoteFile file : files.keySet())
			versions.add( new Versioned<RemoteFile>( file, file.getVersion() ) );
		
		VectorClockInconsistencyResolver<RemoteFile> vec_resolver = new VectorClockInconsistencyResolver<>();
		List<Versioned<RemoteFile>> inconsistency = vec_resolver.resolveConflicts( versions );
		
		// get the uncorrelated files
		List<RemoteFile> uncorrelatedVersions = new ArrayList<>();
		for(Versioned<RemoteFile> version : inconsistency)
			uncorrelatedVersions.add( version.getValue() );
		
		return uncorrelatedVersions;
	}
	
	private void handleGET_ALL( final String clientAddress ) throws IOException
	{
		openClientConnection( clientAddress );
		
		if(session != null) {
			List<DistributedFile> files = fMgr.getDatabase().getAllFiles();
			List<byte[]> filesToSend = new ArrayList<>( files.size() );
			for(DistributedFile file : files) {
				RemoteFile rFile = new RemoteFile( file );
				filesToSend.add( Utils.serializeObject( rFile ) );
			}
			
			MessageResponse message = new MessageResponse( (byte) 0x0, filesToSend );
			session.sendMessage( message, true );
			
			session.close();
		}
	}
	
	private void handleDELETE( final boolean isCoordinator, final DistributedFile file ) throws IOException, SQLException
	{
		file.incrementVersion( _address );
		boolean updated = fMgr.getDatabase().removeFile( file.getName(), file.getVersion() );
		
		if(updated) {
			// send, in parallel, the DELETE request to all the agreed nodes
			List<DistributedFile> files = Collections.singletonList( file );
			for(GossipMember node : agreedNodes)
				fMgr.sendFiles( node.getPort() + 1, Message.DELETE, files, node.getHost(), null, false, null, true );
		}
	}
	
	/** 
	 * Send the actual request to the replica nodes.
	 * 
	 * @param fileName		name of the file to send
	 * 
	 * @return list of sessions opened with other replica nodes.
	*/
	private List<TCPSession> sendRequestToReplicaNodes( final String fileName )
	{
		// send the message to the replica nodes
		List<TCPSession> openSessions = new ArrayList<>();
		byte[] message = Utils.serializeObject( new MessageRequest( Message.GET, fileName ) );
		LOGGER.info( "Send request to replica nodes..." );
		
		for(GossipMember node : agreedNodes) {
			try{
				TCPSession session = _net.tryConnect( node.getHost(), node.getPort(), 2000 );
				if(session != null) {
					session.sendMessage( message, true );
					openSessions.add( session );
				}
			} catch( IOException e ) {}
		}
		
		return openSessions;
	}
	
	private void openClientConnection( final String clientAddress ) throws IOException
	{
		session.close();
		
		LOGGER.info( "Open a direct connection with the client: " + clientAddress );
		session = _net.tryConnect( clientAddress, Utils.SERVICE_PORT, 5000 );
	}
	
	public void setBlocked( final boolean value ) {
		quorum_t.blocked.set( value );
		
		if(getBlocked()) quorum_t.timer.start();
		else quorum_t.timer.stop();
	}
	
	private boolean getBlocked() {
		return quorum_t.blocked.get();
	}
	
	/**
	 * Class used to receive the quorum requests.
	 * The request can be a make or a release quorum.
	*/
	public class QuorumThread extends Thread
	{
		private int port;
		private AtomicBoolean blocked = new AtomicBoolean( false );
		private Timer timer;
		
		/** Time wait for the quorum completion. */
		private static final int BLOCKED_TIME = 300000; // 5 minutes
		private static final byte MAKE_QUORUM = 0, RELEASE_QUORUM = 1;
		private static final byte ACCEPT_QUORUM_REQUEST = 0, DECLINE_QUORUM_REQUEST = 1;
		
		public QuorumThread( final int port ) throws IOException, JSONException
		{
			this.port = port + 3;
			
			timer = new Timer( BLOCKED_TIME, new ActionListener(){
				@Override
				public void actionPerformed( final ActionEvent e )
				{
					setBlocked( false );
				}
			} );
			
			List<GossipMember> nodes = QuorumSystem.loadDecision();
			cancelQuorum( null, nodes );
		}
		
		@Override
		public void run()
		{
			TCPnet net;
			
			try{ net = new TCPnet( _address, port ); }
			catch( IOException e ) {
				return;
			}
			
			while(true) {
				try {
					TCPSession session = net.waitForConnection();
					
					// read the request
					byte[] data = session.receiveMessage();
					LOGGER.info( "[QUORUM] Received a connection from: " + session.getSrcAddress() );
					
					switch( data[0] ) {
						case( MAKE_QUORUM ):
							byte opType = data[1];
							LOGGER.info( "Received a MAKE_QUORUM request. Actual status: " + getBlocked() );
							// send the current blocked state
							boolean blocked = getBlocked();
							data = new byte[]{ (blocked) ? DECLINE_QUORUM_REQUEST : ACCEPT_QUORUM_REQUEST };
							session.sendMessage( data, false );
							//net.sendMessage( data, InetAddress.getByName( net.getSrcAddress() ), net.getSrcPort() );
							LOGGER.info( "Type: " + opType );
							if(opType != Message.GET && !blocked)
								setBlocked( true );
							
							break;
						
						case( RELEASE_QUORUM ):
							LOGGER.info( "Received a RELEASE_QUORUM request" );
							setBlocked( false );
							break;
					}
				}
				catch( IOException e ) {
					e.printStackTrace();
					break;
				}
			}
			
			net.close();
		}

		private List<GossipMember> checkQuorum( final TCPSession session, final byte opType, final String destId ) throws IOException
		{
			ByteBuffer id = Utils.hexToBytes( destId );
			
			List<GossipMember> nodes = getSuccessorNodes( id, _address, QuorumSystem.getMaxNodes() );
			
			LOGGER.debug( "Neighbours: " + nodes.size() );
			if(nodes.size() < QuorumSystem.getMinQuorum( opType )) {
				// if there are a number of nodes less than the quorum,
				// we neither start the protocol.
				sendQuorumResponse( session, Message.TRANSACTION_FAILED );
				return nodes;
			}
			else {
				List<GossipMember> nodeAddress = contactNodes( session, opType, nodes );
				return nodeAddress;
			}
		}

		/**
		 * Contacts the nodes to complete the quorum phase.
		 * 
		 * @param session
		 * @param opType
		 * @param nodes
		 * 
		 * @return list of contacted nodes, that have agreed to the quorum
		*/
		private List<GossipMember> contactNodes( final TCPSession session, final byte opType, final List<GossipMember> nodes ) throws IOException
		{
			int errors = 0;
			List<GossipMember> agreedNodes = new ArrayList<>();
			
			TCPnet net = new TCPnet();
			
			for(GossipMember node : nodes) {
				LOGGER.info( "[SN] Contacting " + node.getHost() + "..." );
				TCPSession mySession = null;
				try {
					mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
					mySession.sendMessage( new byte[]{ MAKE_QUORUM, opType }, false );
					
					LOGGER.info( "[SN] Waiting the response..." );
					byte[] data = mySession.receiveMessage();
					mySession.close();
					
					LOGGER.debug( "[SN] Response: " + data[0] );
					if(data[0] == ACCEPT_QUORUM_REQUEST) {
						LOGGER.info( "[SN] Node " + node.getHost() + " agree to the quorum." );
						// not blocked => agree to the quorum
						agreedNodes.add( node );
						QuorumSystem.saveDecision( agreedNodes );
					}
					else {
						// blocked => the node doesn't agree to the quorum
						LOGGER.info( "[SN] Node " + node.getHost() + " doesn't agree to the quorum." );
						if(QuorumSystem.unmakeQuorum( ++errors, opType )) {
							cancelQuorum( session, agreedNodes );
							break;
						}
					}
				}
				catch( IOException | JSONException e ) {
					if(mySession != null)
						mySession.close();
					
					LOGGER.info( "[SN] Node " + node.getHost() + " is not reachable." );
					if(QuorumSystem.unmakeQuorum( ++errors, opType )) {
						cancelQuorum( session, agreedNodes );
						break;
					}
				}
			}
			
			net.close();
			
			return agreedNodes;
		}
		
		/**
		 * Close the already opened quorum requests.
		 * 
		 * @param session
		 * @param agreedNodes	
		*/
		private void cancelQuorum( final TCPSession session, final List<GossipMember> agreedNodes ) throws IOException
		{
			if(session != null)
				LOGGER.info( "[SN] The quorum cannot be reached. The transaction will be closed." );
			
			TCPnet net = new TCPnet();
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				GossipMember node = agreedNodes.get( i );
				TCPSession mySession = null;
				try {
					mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
					mySession.sendMessage( new byte[]{ RELEASE_QUORUM }, false );
					agreedNodes.remove( i );
					QuorumSystem.saveDecision( agreedNodes );
				}
				catch( IOException | JSONException e ) {
					//e.printStackTrace();
				}
				
				if(mySession != null)
					mySession.close();
			}
			
			net.close();
			
			// send to the client the negative response
			sendQuorumResponse( session, Message.TRANSACTION_FAILED );
		}
		
		/**
		 * Sends to the client the quorum response.
		 * 
		 * @param session
		 * @param response
		*/
		private void sendQuorumResponse( final TCPSession session, final byte response ) throws IOException
		{
			if(session != null) {
				MessageResponse message = new MessageResponse( response );
				session.sendMessage( message, true );
			}
		}
	}

	public static void main( String args[] ) throws Exception
	{
		new StorageNode();
	}
}