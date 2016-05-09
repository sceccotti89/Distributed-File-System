/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import distributed_fs.exception.DFSException;
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
import distributed_fs.utils.CmdLineParser;
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.VectorClockInconsistencyResolver;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;
import gossiping.GossipService;
import gossiping.GossipSettings;
import gossiping.RemoteGossipMember;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public class StorageNode extends DFSnode
{
	private static GossipMember me;
	private QuorumThread quorum_t;
	
	// ===== Used by the private constructor ===== //
	private TCPSession session;
	private String destId; // Destination node identifier, for an input request.
	private List<QuorumNode> agreedNodes; // List of nodes that have agreed to the quorum.
	// =========================================== //
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.
	 * 
	 * @param startupMembers		list of nodes
	 * @param resourcesLocation		the root where the resources are taken from.
	 * 								If {@code null} the default one will be selected ({@link Utils#RESOURCE_LOCATION});
	 * @param databaseLocation		the root where the database is located.
	 * 								If {@code null} the default one will be selected ({@link Utils#});
	*/
	public StorageNode( final List<GossipMember> startupMembers,
						final String resourcesLocation,
						final String databaseLocation ) throws IOException, JSONException, InterruptedException, DFSException
	{
		super( GossipMember.STORAGE, startupMembers );
		
		if(runner != null) {
			me = runner.getGossipService().getGossipManager().getMyself();
			this.port = me.getPort();
		}
		else {
			// Start the gossiping from the input list.
			this.port = GossipManager.GOSSIPING_PORT;
			
			String id = Utils.bytesToHex( Utils.getNodeId( 1, _address ).array() );
			me = new RemoteGossipMember( _address, this.port, id, computeVirtualNodes(), GossipMember.STORAGE );
			
			GossipSettings settings = new GossipSettings();
			GossipService gossipService = new GossipService( _address, me.getPort(), me.getId(), me.getVirtualNodes(),
															 me.getNodeType(), startupMembers, settings, this );
			gossipService.start();
		}
		
		cHasher.addBucket( me, me.getVirtualNodes() );
		
		fMgr = new FileManagerThread( me, this.port + 1, cHasher, resourcesLocation, databaseLocation );
		fMgr.addStorageNode( this );
		fMgr.start();
		
		monitor = new NetworkMonitorSenderThread( _address, this );
		monitor.start();
		
		threadPool.execute( quorum_t = new QuorumThread( port ) );
		
		//this.port = Utils.SERVICE_PORT;
		//this.port = port;
		
		try {
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//System.out.println( "[SN] Waiting on: " + _address + ":" + this.port );
				TCPSession session = _net.waitForConnection( _address, this.port );
				if(session != null)
					threadPool.execute( new StorageNode( fMgr, quorum_t, cHasher, _net, session ) );
			}
		}
		catch( IOException e ) {}
		
		System.out.println( "[SN] '" + _address + ":" + port + "' Closed." );
		//closeResources();
	}
	
	/** Testing. */
	public StorageNode( final List<GossipMember> startupMembers,
						final String id,
						final String address,
						final int port,
						final String resourcesLocation,
						final String databaseLocation ) throws IOException, JSONException, InterruptedException, DFSException
	{
		super();
		
		_address = address;
		
		for(GossipMember member: startupMembers) {
			if(member.getNodeType() != GossipMember.LOAD_BALANCER)
				gossipEvent( member, GossipState.UP );
		}
		
		fMgr = new FileManagerThread( new RemoteGossipMember( _address, port, id, 3, GossipMember.STORAGE ), port + 1,
									  cHasher, resourcesLocation, databaseLocation );
		fMgr.addStorageNode( this );
		monitor = new NetworkMonitorSenderThread( _address, this );
		
		this.port = port;
	}
	
	@Test
	public void launch() throws JSONException
	{
		fMgr.start();
		
		monitor.start();
		try {
			threadPool.execute( quorum_t = new QuorumThread( port ) );
			
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//LOGGER.debug( "[SN] Waiting on: " + _address + ":" + port );
				TCPSession session = _net.waitForConnection( _address, port );
				if(session != null)
					threadPool.execute( new StorageNode( fMgr, quorum_t, cHasher, _net, session ) );
			}
		}
		catch( IOException e ) {}
		
		System.out.println( "[SN] '" + _address + ":" + port + "' Closed." );
		//closeResources();
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
			//ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
			MessageRequest data = Utils.deserializeObject( session.receiveMessage() );
			//byte opType = data.get();
			//boolean isCoordinator = (data.get() == (byte) 0x1);
			byte opType = data.getType();
			boolean isCoordinator = data.startQuorum();
			LOGGER.debug( "[SN] Received: " + getCodeString( opType ) + ":" + isCoordinator );
			
			if(isCoordinator) { // PUT or DELETE operation
				// the connection with the client must be estabilished before the quorum
				//openClientConnection( data );
				openClientConnection( data.getClientAddress() );
				
				// get the destination id, since it can be a virtual node
				//destId = new String( Utils.getNextBytes( data ), StandardCharsets.UTF_8 );
				destId = data.getDestId();
				
				LOGGER.info( "[SN] Start the quorum..." );
				agreedNodes = quorum_t.checkQuorum( session, opType, destId );
				int replicaNodes = agreedNodes.size();
				// TODO rimuovere i commenti di TEST dalla classe QuorumSystem
				
				// checks if the quorum has been completed successfully
				if(!QuorumSystem.isQuorum( opType, replicaNodes )) {
					LOGGER.info( "[SN] Quorum failed: " + replicaNodes + "/" + QuorumSystem.getMinQuorum( opType ) );
					//session.sendMessage( new byte[]{ Utils.TRANSACTION_FAILED }, false );
					//MessageResponse message = new MessageResponse( Utils.TRANSACTION_FAILED );
					//session.sendMessage( message, true );
					session.close();
					stats.decreaseValue( NodeStatistics.NUM_CONNECTIONS );
					return;
				}
				else {
					if(opType != Message.GET) {
						LOGGER.info( "[SN] Quorum completed succesfully: " + replicaNodes + "/" + QuorumSystem.getMinQuorum( opType ) );
						quorum_t.sendQuorumResponse( session, Message.TRANSACTION_OK );
						//quorum_t.closeQuorum( new ArrayList<>( agreedNodes ) );
						
						//QuorumSystem.saveDecision( agreedNodes );
						//MessageResponse message = new MessageResponse( Utils.TRANSACTION_OK );
						//session.sendMessage( message, true );
					}
					//session.sendMessage( new byte[]{ Utils.TRANSACTION_OK }, false );
				}
			}
			
			switch( opType ) {
				case( Message.PUT ):
					/*RemoteFile file = Utils.deserializeObject( Utils.getNextBytes( data ) );
					
					// get (if present) the hinted handoff address
					String hintedHandoff = null;
					if(data.remaining() > 0)
						hintedHandoff = new String( Utils.getNextBytes( data ) );*/
					//RemoteFile file = Utils.deserializeObject( data.getFile() );
					RemoteFile file = new RemoteFile( data.getFile() );
					
					// get (if present) the hinted handoff address
					String hintedHandoff = data.getHintedHandoff();
					
					LOGGER.debug( "PUT: " + file.getName() + ":" + hintedHandoff );
					
					handlePUT( isCoordinator, file, hintedHandoff );
					break;
					
				case( Message.GET ):
					//handleGET( isCoordinator, new String( Utils.getNextBytes( data ) ) );
					handleGET( isCoordinator, data.getFileName() );
					break;
					
				case( Message.GET_ALL ): 
					//handleGET_ALL( data );
					handleGET_ALL( data.getClientAddress() );
					break;
				
				case( Message.DELETE ):
					//handleDELETE( isCoordinator, Utils.deserializeObject( Utils.getNextBytes( data ) ) );
					//handleDELETE( isCoordinator, Utils.deserializeObject( data.getFile() ) );
					handleDELETE( isCoordinator, new DistributedFile( data.getFile() ) );
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
		System.out.println( "OLD_VERSION: " + file.getVersion() );
		VectorClock newClock = file.getVersion().incremented( _address );
		VectorClock updated = fMgr.getDatabase().saveFile( file, newClock, hintedHandoff, true );
		System.out.println( "UPDATED: " + (updated != null) + ", NEW_VERSION: " + file.getVersion() );
		
		if(updated != null) {
			file.setVersion( updated );
			
			// Send, in parallel, the file to the replica nodes.
			List<DistributedFile> files = Collections.singletonList( new DistributedFile( file, fMgr.getDatabase().getFileSystemRoot() ) );
			for(QuorumNode qNode : agreedNodes) {
				qNode.addList( agreedNodes );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getPort() + 1/*, Message.PUT*/, files, node.getHost(), false, null, qNode );
			}
		}
	}
	
	private void handleGET( final boolean isCoordinator, final String fileName ) throws IOException, JSONException
	{
		if(isCoordinator) {
			// Send the GET request to all the agreed nodes,
			// to retrieve their version of the file and make the reconciliation.
			List<TCPSession> openSessions = sendRequestToReplicaNodes( fileName );
			
			// the replica files can be less than the quorum
			LOGGER.info( "Receive the files from the replica nodes..." );
			HashMap<RemoteFile, byte[]> filesToSend = new HashMap<>( QuorumSystem.getMaxNodes() + 1 );
			int errors = 0;
			
			int index = 0;
			for(TCPSession session : openSessions) {
				try{
					ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
					if(data.get() == (byte) 0x1) { // Replica node owns the requested file.
						byte[] file = Utils.getNextBytes( data );
						filesToSend.put( Utils.deserializeObject( file ), file );
						
						// Update the list of agreedNodes.
						agreedNodes.remove( index );
						QuorumSystem.saveDecision( agreedNodes );
					}
				}
				catch( IOException e ) {
					if(QuorumSystem.unmakeQuorum( ++errors, Message.GET )) {
						LOGGER.info( "[SN] Quorum failed: " + openSessions.size() + "/" + QuorumSystem.getMinQuorum( Message.GET ) );
						//MessageResponse message = new MessageResponse( Utils.TRANSACTION_FAILED );
						//this.session.sendMessage( message, true );
						
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
			
			//quorum_t.closeQuorum( agreedNodes );
			
			// put in the list the file present in the database of this node
			DistributedFile dFile = fMgr.getDatabase().getFile( Utils.getId( fileName ) );
			if(dFile != null) {
				RemoteFile rFile = new RemoteFile( dFile, fMgr.getDatabase().getFileSystemRoot() );
				//byte[] file = Utils.serializeObject( rFile );
				filesToSend.put( rFile, rFile.read() );
			}
			
			// try a first reconciliation
			LOGGER.debug( "Files: " + filesToSend.size() );
			List<RemoteFile> reconciledFiles = makeReconciliation( filesToSend );
			LOGGER.debug( "Files after reconciliation: " + reconciledFiles.size() );
			
			// send the files directly to the client
			/*session.sendMessage( Utils.intToByteArray( reconciledFiles.size() ), false );
			for(int i = 0; i < reconciledFiles.size(); i++) {
				byte[] data = filesToSend.get( reconciledFiles.get( i ) );
				session.sendMessage( data, true );
			}
			sendFilesToClient( reconciledFiles );
			*/
			
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
				//session.sendMessage( new byte[]{ (byte) 0x0 }, false );
				session.sendMessage( new MessageResponse( (byte) 0x0 ), false );
			else {
				//RemoteFile rFile = new RemoteFile( file );
				MessageResponse message = new MessageResponse( (byte) 0x1 );
				//message.addFile( Utils.serializeObject( new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() ) ) );
				message.addFile( new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() ).read() );
				session.sendMessage( message, true );
				//byte[] msg = _net.createMessage( new byte[]{ (byte) 0x1 }, Utils.serializeObject( rFile ), true );
				//session.sendMessage( msg, true );
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
	
	/*private void handleGET_ALL( final ByteBuffer data ) throws IOException
	{
		openClientConnection( data );
		
		if(session != null) {
			List<DistributedFile> files = fMgr.getDatabase().getAllFiles();
			session.sendMessage( Utils.intToByteArray( files.size() ), true );
			for(int i = files.size() - 1; i >= 0; i --) {
				RemoteFile file = new RemoteFile( files.get( i ) );
				session.sendMessage( Utils.serializeObject( file ), true );
			}
			
			session.close();
		}
	}*/
	
	private void handleGET_ALL( final String clientAddress ) throws IOException
	{
		openClientConnection( clientAddress );
		
		if(session != null) {
			List<DistributedFile> files = fMgr.getDatabase().getAllFiles();
			List<byte[]> filesToSend = new ArrayList<>( files.size() );
			for(DistributedFile file : files) {
				RemoteFile rFile = new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() );
				//filesToSend.add( Utils.serializeObject( rFile ) );
				filesToSend.add( rFile.read() );
			}
			
			MessageResponse message = new MessageResponse( (byte) 0x0, filesToSend );
			session.sendMessage( message, true );
			
			session.close();
		}
	}
	
	private void handleDELETE( final boolean isCoordinator, final DistributedFile file ) throws IOException, SQLException
	{
		VectorClock newClock = file.getVersion().incremented( _address );
		VectorClock updated = fMgr.getDatabase().removeFile( file.getName(), newClock, true );
		System.out.println( "UPDATED: " + (updated != null) + ", AGREED_NODES: " + agreedNodes );
		LOGGER.debug( "Deleted file \"" + file.getName() + "\"" );
		
		if(updated != null) {
			file.setVersion( updated );
			file.setDeleted( true );
			
			// Send, in parallel, the DELETE request to all the agreed nodes.
			List<DistributedFile> files = Collections.singletonList( file );
			for(QuorumNode qNode : agreedNodes) {
				qNode.addList( agreedNodes );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getPort() + 1/*, Message.DELETE*/, files, node.getHost(), false, null, qNode );
			}
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
		// create the message
		/*ByteBuffer buffer = ByteBuffer.allocate( Byte.BYTES * 2 + Integer.BYTES + fileName.length() );
		buffer.put( Utils.GET ).put( (byte) 0x0 ).putInt( fileName.length() ).put( fileName.getBytes( StandardCharsets.UTF_8 ) );
		byte[] data = buffer.array();
		
		// send the message to the replica nodes
		List<TCPSession> openSessions = new ArrayList<>();
		LOGGER.info( "Send request to replica nodes..." );
		
		for(String address : agreedNodes) {
			try{
				TCPSession session = _net.tryConnect( address, Utils.SERVICE_PORT, 2000 );
				if(session != null) {
					session.sendMessage( data, true );
					openSessions.add( session );
				}
			} catch( IOException e ) {}
		}*/
		
		// send the message to the replica nodes
		List<TCPSession> openSessions = new ArrayList<>();
		byte[] message = Utils.serializeObject( new MessageRequest( Message.GET, fileName ) );
		LOGGER.info( "Send request to replica nodes..." );
		
		for(QuorumNode qNode : agreedNodes) {
			try{
				GossipMember node = qNode.getNode();
				TCPSession session = _net.tryConnect( node.getHost(), node.getPort(), 2000 );
				if(session != null) {
					session.sendMessage( message, true );
					openSessions.add( session );
				}
			} catch( IOException e ) {}
		}
		
		return openSessions;
	}
	
	/**
	 * Opens a connection with the client.
	*/
	/*private void openClientConnection( final ByteBuffer data ) throws IOException
	{
		session.close();
		
		String clientAddress = new String( Utils.getNextBytes( data ), StandardCharsets.UTF_8 );
		LOGGER.info( "Open a direct connection with the client: " + clientAddress );
		session = _net.tryConnect( clientAddress, Utils.SERVICE_PORT, 5000 );
	}*/
	
	private void openClientConnection( final String clientAddress ) throws IOException
	{
		session.close();
		
		LOGGER.info( "Open a direct connection with the client: " + clientAddress );
		session = _net.tryConnect( clientAddress, Utils.SERVICE_PORT, 5000 );
	}
	
	public void setBlocked( final boolean value, final long id )
	{
		quorum_t.blocked.set( value );
		
		if(getBlocked()) quorum_t.timer.start();
		else {
			if(id == quorum_t.getId())
				quorum_t.timer.stop();
		}
	}
	
	private boolean getBlocked()
	{
		return quorum_t.blocked.get();
	}
	
	/**
	 * Class used to manage the agreed nodes of the quorum.
	*/
	public static class QuorumNode
	{
		private GossipMember node;
		private List<QuorumNode> nodes;
		private long id;
		
		public QuorumNode( final GossipMember node, final long id )
		{
			this.node = node;
			this.id = id;
		}
		
		public GossipMember getNode()
		{
			return node;
		}
		
		/**
		 * Method used, during the transmission of the files,
		 * to set the list of agreed nodes.
		*/
		public void addList( final List<QuorumNode> nodes )
		{
			this.nodes = nodes;
		}
		
		public List<QuorumNode> getList()
		{
			return nodes;
		}
		
		public long getId()
		{
			return id;
		}
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
		private Long id = (long) -1;
		
		/** Time wait for the quorum completion. */
		private static final int BLOCKED_TIME = 60000; // 1 minute.
		private static final byte MAKE_QUORUM = 0, RELEASE_QUORUM = 1;
		private static final byte ACCEPT_QUORUM_REQUEST = 0, DECLINE_QUORUM_REQUEST = 1;
		//private static final int QUORUM_PORT = 2500;
		
		public QuorumThread( final int port ) throws IOException, JSONException
		{
			this.port = port + 3;
			
			timer = new Timer( BLOCKED_TIME, new ActionListener(){
				@Override
				public void actionPerformed( final ActionEvent e )
				{
					setBlocked( false, id );
				}
			} );
			
			List<QuorumNode> nodes = QuorumSystem.loadDecision();
			if(nodes.size() > 0 && QuorumSystem.timeElapsed < BLOCKED_TIME)
				cancelQuorum( null, nodes );
		}
		
		@Override
		public void run()
		{
			/*UDPnet net;
			
			//try{ net = new UDPnet( _address, QUORUM_PORT ); }
			try{ net = new UDPnet( _address, port ); }
			catch( IOException e ) {
				return;
			}*/
			
			TCPnet net;
			
			//try{ net = new UDPnet( _address, QUORUM_PORT ); }
			try{
				net = new TCPnet( _address, port );
				net.setSoTimeout( WAIT_CLOSE );
			}
			catch( IOException e ) {
				e.printStackTrace();
				return;
			}
			
			while(!shutDown) {
				try {
					//LOGGER.info( "[QUORUM] Waiting on " + _address + ":" + port );
					TCPSession session = net.waitForConnection();
					if(session == null)
						continue;
					
					// read the request
					//byte[] data = net.receiveMessage();
					//LOGGER.info( "[QUORUM] Received a connection from: " + net.getSrcAddress() );
					
					byte[] data = session.receiveMessage();
					LOGGER.info( "[QUORUM] Received a connection from: " + session.getSrcAddress() );
					
					switch( data[0] ) {
						case( MAKE_QUORUM ):
							byte opType = data[1];
							LOGGER.info( "Received a MAKE_QUORUM request. Actual status: " + getBlocked() );
							// send the current blocked state
							boolean blocked = getBlocked();
							if(!blocked)
								data = net.createMessage( new byte[]{ ACCEPT_QUORUM_REQUEST }, getNextId(), false );
							else
								data = new byte[]{ DECLINE_QUORUM_REQUEST };
							session.sendMessage( data, false );
							
							//net.sendMessage( data, InetAddress.getByName( net.getSrcAddress() ), net.getSrcPort() );
							//LOGGER.info( "[QUORUM] Type: " + getCodeString( opType ) );
							if(opType != Message.GET && !blocked)
								setBlocked( true, id );
							
							break;
						
						case( RELEASE_QUORUM ):
							LOGGER.info( "Received a RELEASE_QUORUM request" );
							setBlocked( false, id );
							break;
					}
				}
				catch( IOException e ) {
					//e.printStackTrace();
					break;
				}
			}
			
			net.close();
			
			LOGGER.info( "Quorum thread closed." );
		}
		
		private byte[] getNextId()
		{
			synchronized( id ) {
				id = (id + 1) % Long.MAX_VALUE;
				return Utils.longToByteArray( id );
			}
		}
		
		public long getId()
		{
			synchronized( id ) {
				return id;
			}
		}
		
		public void close()
		{
			interrupt();
		}

		private List<QuorumNode> checkQuorum( final TCPSession session, final byte opType, final String destId ) throws IOException
		{
			ByteBuffer id = Utils.hexToBytes( destId );
			
			List<GossipMember> nodes = getSuccessorNodes( id, _address, QuorumSystem.getMaxNodes() );
			
			LOGGER.debug( "Neighbours: " + nodes.size() );
			if(nodes.size() < QuorumSystem.getMinQuorum( opType )) {
				// if there are a number of nodes less than the quorum,
				// we neither start the protocol.
				sendQuorumResponse( session, Message.TRANSACTION_FAILED );
				
				List<QuorumNode> qNodes = new ArrayList<>( nodes.size() );
				for(GossipMember node : nodes)
					qNodes.add( new QuorumNode( node, 0 ) );
				
				return qNodes;
			}
			else {
				List<QuorumNode> nodeAddress = contactNodes( session, opType, nodes );
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
		private List<QuorumNode> contactNodes( final TCPSession session, final byte opType, final List<GossipMember> nodes ) throws IOException
		{
			int errors = 0;
			List<QuorumNode> agreedNodes = new ArrayList<>();
			
			//UDPnet net = new UDPnet();
			TCPnet net = new TCPnet();
			//net.setSoTimeout( 2000 );
			
			for(GossipMember node : nodes) {
				LOGGER.info( "[SN] Contacting " + node + "..." );
				TCPSession mySession = null;
				try {
					mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
					mySession.sendMessage( new byte[]{ MAKE_QUORUM, opType }, false );
					
					//net.sendMessage( new byte[]{ MAKE_QUORUM }, InetAddress.getByName( node.getHost() ), QUORUM_PORT );
					//net.sendMessage( new byte[]{ MAKE_QUORUM }, InetAddress.getByName( node.getHost() ), node.getPort() + 3 );
					LOGGER.info( "[SN] Waiting the response..." );
					//byte[] data = net.receiveMessage();
					ByteBuffer data = ByteBuffer.wrap( mySession.receiveMessage() );
					mySession.close();
					
					if(data.get() == ACCEPT_QUORUM_REQUEST) {
						LOGGER.info( "[SN] Node " + node + " agree to the quorum." );
						// not blocked => agree to the quorum
						agreedNodes.add( new QuorumNode( node, data.getLong() ) );
						QuorumSystem.saveDecision( agreedNodes );
					}
					else {
						// blocked => the node doesn't agree to the quorum
						LOGGER.info( "[SN] Node " + node + " doesn't agree to the quorum." );
						if(QuorumSystem.unmakeQuorum( ++errors, opType )) {
							cancelQuorum( session, agreedNodes );
							break;
						}
					}
				}
				catch( IOException | JSONException e ) {
					if(mySession != null)
						mySession.close();
					
					LOGGER.info( "[SN] Node " + node + " is not reachable." );
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
		 * Closes the opened quorum requests.
		 * 
		 * @param session		network channel with the client
		 * @param agreedNodes	list of contacted nodes
		*/
		private void cancelQuorum( final TCPSession session, final List<QuorumNode> agreedNodes ) throws IOException
		{
			if(session != null)
				LOGGER.info( "[SN] The quorum cannot be reached. The transaction will be closed." );
			
			closeQuorum( agreedNodes );
			// send to the client the negative response
			sendQuorumResponse( session, Message.TRANSACTION_FAILED );
		}
		
		private void closeQuorum( final List<QuorumNode> agreedNodes )
		{
			//UDPnet net = new UDPnet();
			TCPnet net = new TCPnet();
			//System.out.println( "[SN] NODES:" + agreedNodes );
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				GossipMember node = agreedNodes.get( i ).getNode();
				TCPSession mySession = null;
				//try { net.sendMessage( new byte[]{ RELEASE_QUORUM }, InetAddress.getByName( node.getHost() ), QUORUM_PORT ); }
				try {
					mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
					//net.sendMessage( new byte[]{ RELEASE_QUORUM }, InetAddress.getByName( node.getHost() ), node.getPort() + 3 );
					mySession.sendMessage( new byte[]{ RELEASE_QUORUM }, false );
					agreedNodes.remove( i );
					//{"members":[{"port":8101,"host":"192.168.1.101"}]}
					QuorumSystem.saveDecision( agreedNodes );
				}
				catch( IOException | JSONException e ) {
					//e.printStackTrace();
				}
				
				if(mySession != null)
					mySession.close();
			}
			
			net.close();
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
		// TODO inserire anche il resource location e il database location tra i possibili input
		List<GossipMember> members = null;
		CmdLineParser.parseArgs( args );
		members = CmdLineParser.getNodes( "n" );
		
		new StorageNode( members, null, null );
	}
}