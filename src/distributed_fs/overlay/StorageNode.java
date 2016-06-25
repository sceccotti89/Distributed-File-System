/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.json.JSONException;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.exception.DFSException;
import distributed_fs.net.NetworkMonitor;
import distributed_fs.net.NetworkMonitorSenderThread;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.overlay.manager.MembershipManagerThread;
import distributed_fs.overlay.manager.QuorumThread;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadState;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransferThread;
import distributed_fs.storage.RemoteFile;
import distributed_fs.utils.ArgumentsParser;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.VersioningUtils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;
import gossiping.GossipService;
import gossiping.GossipSettings;
import gossiping.RemoteGossipMember;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;

public class StorageNode extends DFSNode
{
	private static GossipMember me;
	private QuorumThread quorum_t;
	private String resourcesLocation;
	
	// ===== Used by the node instance ===== //
	private TCPSession session;
	private String destId; // Destination node identifier, for an input request.
	private List<QuorumNode> agreedNodes; // List of nodes that have agreed to the quorum.
	private QuorumSession qSession;
	private boolean replacedThread;
	// =========================================== //
	
	private MembershipManagerThread lMgr_t;
	private List<Thread> threadsList;
	
	// Used to create the list of actions done by the node.
	private static final Object DONE = new Object();
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.<br>
	 * The node starts only using the {@linkplain #launch()}
	 * method.
	 * 
	 * @param address				the ip address. If {@code null} it will be taken using the configuration file parameters.
	 * @param startupMembers		list of nodes
	 * @param resourcesLocation		the root where the resources are taken from.
	 * 								If {@code null} the default one will be selected ({@link DFSUtils#RESOURCE_LOCATION});
	 * @param databaseLocation		the root where the database is located.
	 * 								If {@code null} the default one will be selected ({@link Utils#});
	*/
	public StorageNode( final String address,
	                    final List<GossipMember> startupMembers,
	                    final String resourcesLocation,
	                    final String databaseLocation ) throws IOException, JSONException, InterruptedException, DFSException
	{
		super( GossipMember.STORAGE, address, startupMembers );
		
		if(runner != null) {
			me = runner.getGossipService().getGossipManager().getMyself();
			this.port = me.getPort();
			lMgr_t = new MembershipManagerThread( _address, this.port, me,
			                                runner.getGossipService().getGossipManager() );
		}
		else {
			// Start the gossiping from the input list.
			this.port = GossipManager.GOSSIPING_PORT;
			
			String id = DFSUtils.getNodeId( 1, _address );
			me = new RemoteGossipMember( _address, this.port, id, computeVirtualNodes(), GossipMember.STORAGE );
			
			GossipSettings settings = new GossipSettings();
			GossipService gossipService = new GossipService( _address, me.getPort(), me.getId(), me.getVirtualNodes(),
															 me.getNodeType(), startupMembers, settings, this );
			gossipService.start();
			lMgr_t = new MembershipManagerThread( _address, this.port, me,
			                                gossipService.getGossipManager() );
		}
		
		cHasher.addBucket( me, me.getVirtualNodes() );
		quorum_t = new QuorumThread( port, _address, this );
		
		fMgr = new FileTransferThread( me, this.port + 1, cHasher, quorum_t, resourcesLocation, databaseLocation );
		
		netMonitor = new NetworkMonitorSenderThread( _address, this );
		
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
	}
	
	/** Testing. */
	public StorageNode( final List<GossipMember> startupMembers,
						final String id,
						final int nodeIndex,
						final String address,
						final int port,
						final String resourcesLocation,
						final String databaseLocation ) throws IOException, JSONException, DFSException
	{
		super();
		
		_address = address;
		me = new RemoteGossipMember( _address, port, id, 3, GossipMember.STORAGE );
		
		for(GossipMember member : startupMembers) {
			if(member.getNodeType() != GossipMember.LOAD_BALANCER)
				gossipEvent( member, GossipState.UP );
		}
		
		// Used for the quorum location.
		this.resourcesLocation = "./Servers/QuorumSessions" + nodeIndex + "/";
		
		quorum_t = new QuorumThread( port, _address, this );
		fMgr = new FileTransferThread( me, port + 1, cHasher, quorum_t, resourcesLocation, databaseLocation );
		netMonitor = new NetworkMonitorSenderThread( _address, this );
		
		lMgr_t = new MembershipManagerThread( address, port, startupMembers );
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
		
		this.port = port;
	}
	
	/**
	 * Disable the anti-entropy mechanism.
	*/
	public void disableAntiEntropy() {
	    fMgr.disableAntiEntropy();
	}
	
	/**
	 * Start the node.
	*/
	public void launch() throws JSONException
	{
		fMgr.start();
		netMonitor.start();
		quorum_t.start();
		monitor_t.start();
		lMgr_t.start();
		
		try {
			_net.setSoTimeout( WAIT_CLOSE );
			while(!shutDown) {
				//LOGGER.debug( "[SN] Waiting on: " + _address + ":" + port );
				TCPSession session = _net.waitForConnection( _address, port );
				if(session != null) {
				    synchronized( threadPool ) {
				        if(threadPool.isShutdown())
				            break;
				        
				        StorageNode node = new StorageNode( getNextThreadID(), false, resourcesLocation, fMgr,
				                                            quorum_t, cHasher, _net, session, netMonitor );
				        monitor_t.addThread( node );
				        
				        threadPool.execute( node );
				    }
				}
				
			    // Check if the monitor thread is alive: if not a new instance is activated.
			    if(!monitor_t.isAlive()) {
			        monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port );
			        monitor_t.start();
			    }
			}
		}
		catch( IOException e ) {
		    //e.printStackTrace();
		}
		
		System.out.println( "[SN] '" + _address + ":" + port + "' Closed." );
	}
	
	/**
     * Constructor used to handle an incoming request.
     * 
     * @param id                    the associated identifier
     * @param replacedThread        {@code true} if the thread replace an old one, {@code false} otherwise
     * @param resourcesLocation     the resource location path
     * @param fMgr                  the file manager thread
     * @param quorum_t              the quorum thread
     * @param cHasher               the consistent hashing structure
     * @param net                   the current TCP channel
     * @param session               the TCP session
     * @param netMonitor            the network monitor
    */
    private StorageNode( final long id,
                         final boolean replacedThread,
                         final String resourcesLocation,
                         final FileTransferThread fMgr,
                         final QuorumThread quorum_t,
                         final ConsistentHasherImpl<GossipMember, String> cHasher,
                         final TCPnet net,
                         final TCPSession session,
                         final NetworkMonitor netMonitor ) throws IOException, JSONException
    {
    	super( net, fMgr, cHasher );
    	setId( id );
    	
    	this.replacedThread = replacedThread;
    	this.resourcesLocation = resourcesLocation;
    	this.quorum_t = quorum_t;
    	this.session = session;
    	this.netMonitor = netMonitor;
    	
    	actionsList = new ArrayDeque<>( 16 );// TODO per adesso e' 16 poi si vedra'
    	state = new ThreadState( id, actionsList, fMgr, quorum_t, cHasher, this.netMonitor );
    }

    @Override
	public void run()
	{
		LOGGER.info( "[SN] Received a connection from: " + session.getSrcAddress() );
		if(!actionsList.isEmpty())
		    actionsList.removeFirst();
		else {
		    stats.increaseValue( NodeStatistics.NUM_CONNECTIONS );
		    actionsList.addLast( DONE );
		}
		
		// TODO GENERARE LA MACCHINA A STATI FINITI, ricordandosi di salvare tutti i dati nell'apposito threadState
		
		// TODO usare il controllo "if(!replacedThread || actionsList.isEmpty())" per testare se deve leggere dalla sessione
		
		try {
		    MessageRequest data;
		    // TODO usare una giusta chiave per il messaggio
		    if(!replacedThread || actionsList.isEmpty()) {
		        data = DFSUtils.deserializeObject( session.receiveMessage() );
		        state.addValue( "", data );
		    }
		    else {
		        data = (MessageRequest) state.getValue( "" );
		        actionsList.removeFirst();
		    }
		    
            byte opType = data.getType();
            String fileName = data.getFileName();
            boolean isCoordinator = data.startQuorum();
            long fileId = (opType == Message.GET_ALL || isCoordinator) ?
                          -1 : DFSUtils.bytesToLong( data.getPayload() );
            Metadata meta = data.getMetadata();
			
			LOGGER.debug( "[SN] Received (TYPE, COORD) = ('" + getCodeString( opType ) + ":" + isCoordinator + "')" );
			
			if(isCoordinator) {
				// The connection with the client must be estabilished before the quorum.
				openClientConnection( meta.getClientAddress() );
				
				// Get the destination id, since it can be any virtual node.
				destId = data.getDestId();
				
				LOGGER.info( "[SN] Start the quorum..." );
				qSession = new QuorumSession( resourcesLocation, id );
				agreedNodes = quorum_t.checkQuorum( session, qSession, opType, fileName, destId );
				int replicaNodes = agreedNodes.size();
				
				// Check if the quorum has been completed successfully.
				if(!QuorumSession.isQuorum( opType, replicaNodes )) {
				    LOGGER.info( "[SN] Quorum failed: " + replicaNodes + "/" + QuorumSession.getMinQuorum( opType ) );
				    closeWorker();
					return;
				}
				else {
					if(opType != Message.GET) {
					    // The successfull transaction will be sent later for the GET operation.
						LOGGER.info( "[SN] Quorum completed successfully: " + replicaNodes + "/" + QuorumSession.getMinQuorum( opType ) );
						quorum_t.sendQuorumResponse( session, Message.TRANSACTION_OK );
					}
				}
			}
			
			switch( opType ) {
				case( Message.PUT ):
					RemoteFile file = new RemoteFile( data.getPayload() );
					// Get (if present) the hinted handoff address.
					String hintedHandoff = meta.getHintedHandoff();
					LOGGER.debug( "[SN] PUT -> (FILE, HH) = ('" + file.getName() + ":" + hintedHandoff + "')" );
					
					handlePUT( isCoordinator, file, hintedHandoff );
					break;
					
				case( Message.GET ):
					handleGET( isCoordinator, fileName, fileId );
					break;
					
				case( Message.GET_ALL ): 
                    handleGET_ALL( meta.getClientAddress() );
                    break;
				
				case( Message.DELETE ):
				    DistributedFile dFile = DFSUtils.deserializeObject( data.getPayload() );
				    // Get (if present) the hinted handoff address.
                    hintedHandoff = meta.getHintedHandoff();
                    LOGGER.debug( "[SN] DELETE: " + dFile.getName() + ":" + hintedHandoff );
				    
				    handleDELETE( isCoordinator, dFile, hintedHandoff );
				    break;
			}
		}
		catch( IOException | SQLException | JSONException e ) {
			e.printStackTrace();
		}
		
		closeWorker();
	}
	
	private void handlePUT( final boolean isCoordinator, final RemoteFile file, final String hintedHandoff ) throws IOException, SQLException
	{
		//LOGGER.debug( "GIVEN_VERSION: " + file.getVersion() + ", NEW_GIVEN_VERSION: " + file.getVersion().incremented( _address ) );
		VectorClock clock = file.getVersion().incremented( _address );
		clock = fMgr.getDatabase().saveFile( file, clock, hintedHandoff, true );
		LOGGER.debug( "[SN] UPDATED: " + (clock != null) );
		
		if(clock == null) // Not updated.
			quorum_t.closeQuorum( qSession, agreedNodes );
		else {
			file.setVersion( clock );
			
			// Send, in parallel, the file to the replica nodes.
			List<DistributedFile> files = Collections.singletonList( new DistributedFile( file, file.isDirectory(), hintedHandoff ) );
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				QuorumNode qNode = agreedNodes.get( i );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getHost(), node.getPort() + 1, files, false, null, qNode );
			}
		}
		
		LOGGER.debug( "Sending the updated clock to the client..." );
		// Send the update state to the client.
		MessageResponse message = new MessageResponse( (byte) ((clock != null) ? 0x1 : 0x0) );
		if(clock != null)
		    message.addObject( DFSUtils.serializeObject( clock ) );
		session.sendMessage( message, true );
		LOGGER.debug( "Clock sent." );
	}
	
	private void handleGET( final boolean isCoordinator, final String fileName, final long fileId ) throws IOException, JSONException
	{
		if(isCoordinator) {
			// Send the GET request to all the agreed nodes,
			// to retrieve their version of the file and make the reconciliation.
			List<TCPSession> openSessions = sendRequestToReplicaNodes( fileName );
			
			// The replica files can be less than the quorum.
			LOGGER.info( "Receive the files from the replica nodes..." );
			HashMap<RemoteFile, byte[]> filesToSend = new HashMap<>( QuorumSession.getMaxNodes() + 1 );
			int errors = 0;
			
			// Get the replica versions.
			int index = 0, offset = 0;
			for(TCPSession session : openSessions) {
				try{
					ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
					if(data.get() == (byte) 0x1) { // Replica node owns the requested file.
					    System.out.println( "NUOVO FILE " + fileName );
					    byte[] file = DFSUtils.getNextBytes( data );
					    filesToSend.put( new RemoteFile( file ), file );
						
						// Update the list of agreedNodes.
						agreedNodes.remove( index - offset );
						qSession.saveState( agreedNodes );
						offset++;
					}
				}
				catch( IOException e ) {
					if(QuorumSession.unmakeQuorum( ++errors, Message.GET )) {
						LOGGER.info( "[SN] Quorum failed: " + openSessions.size() + "/" + QuorumSession.getMinQuorum( Message.GET ) );
						quorum_t.cancelQuorum( this.session, qSession, agreedNodes );
						return;
					}
				}
				
				index++;
				session.close();
			}
			
			// Send the positive notification to the client.
			LOGGER.info( "[SN] Quorum completed successfully: " + openSessions.size() + "/" + QuorumSession.getMinQuorum( Message.GET ) );
			quorum_t.sendQuorumResponse( session, Message.TRANSACTION_OK );
			
			// Put in the list the file present in the database of this node.
			DistributedFile dFile = fMgr.getDatabase().getFile( fileName );
			if(dFile != null) {
				RemoteFile rFile = new RemoteFile( dFile, fMgr.getDatabase().getFileSystemRoot() );
				filesToSend.put( rFile, rFile.read() );
			}
			
			// Try a first reconciliation.
			LOGGER.debug( "Files: " + filesToSend.size() );
			List<RemoteFile> reconciledFiles = makeReconciliation( filesToSend );
			LOGGER.debug( "Files after reconciliation: " + reconciledFiles.size() );
			
			// Send the files directly to the client.
			MessageResponse message = new MessageResponse();
			for(int i = 0; i < reconciledFiles.size(); i++) {
				byte[] data = filesToSend.get( reconciledFiles.get( i ) );
				message.addObject( data );
			}
			
			session.sendMessage( message, true );
			
			LOGGER.info( "Files sent to the client." );
		}
		else {
			// REPLICA node: send the requested file to the coordinator.
		    DistributedFile file = fMgr.getDatabase().getFile( fileName );
		    System.out.println( "[SN] Cerco il file: " + fileName + ": " + file );
		    byte[] message;
		    if(file == null)
				message = new byte[]{ (byte) 0x0 };
			else {
			    byte[] bFile = new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() ).read();
			    message = _net.createMessage( new byte[]{ (byte) 0x1 }, bFile, true );
				//message.addObject( new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() ).read() );
			}
			
		    // Remove the lock to the file.
			quorum_t.setLocked( false, fileName, fileId, Message.GET );
			session.sendMessage( message, true );
		}
	}
	
	/**
	 * Makes the reconciliation among different vector clocks.
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
		
		//VectorClockInconsistencyResolver<RemoteFile> vec_resolver = new VectorClockInconsistencyResolver<>();
		//List<Versioned<RemoteFile>> inconsistency = vec_resolver.resolveConflicts( versions );
		List<Versioned<RemoteFile>> inconsistency = VersioningUtils.resolveVersions( versions );
		
		// Get the uncorrelated files.
		List<RemoteFile> uncorrelatedVersions = new ArrayList<>();
		for(Versioned<RemoteFile> version : inconsistency)
			uncorrelatedVersions.add( version.getValue() );
		
		return uncorrelatedVersions;
	}
	
	private void handleGET_ALL( final String clientAddress ) throws IOException
    {
        openClientConnection( clientAddress );
        
        List<DistributedFile> files = fMgr.getDatabase().getAllFiles();
        List<byte[]> filesToSend = new ArrayList<>( files.size() );
        for(DistributedFile file : files) {
            RemoteFile rFile = new RemoteFile( file, fMgr.getDatabase().getFileSystemRoot() );
            //filesToSend.add( Utils.serializeObject( rFile ) );
            filesToSend.add( rFile.read() );
        }
        
        MessageResponse message = new MessageResponse( (byte) 0x0, filesToSend );
        session.sendMessage( message, true );
    }
	
	private void handleDELETE( final boolean isCoordinator, final DistributedFile file, final String hintedHandoff ) throws IOException, SQLException
	{
		VectorClock clock = file.getVersion().incremented( _address );
		clock = fMgr.getDatabase().removeFile( file.getName(), clock, hintedHandoff );
		//System.out.println( "UPDATED: " + (clock != null) + ", AGREED_NODES: " + agreedNodes );
		
		if(clock == null)
			quorum_t.closeQuorum( qSession, agreedNodes );
		else {
		    LOGGER.debug( "Deleted file \"" + file.getName() + "\"" );
			file.setVersion( clock );
			file.setDeleted( true );
			
			// Send, in parallel, the DELETE request to all the agreed nodes.
			List<DistributedFile> files = Collections.singletonList( file );
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				QuorumNode qNode = agreedNodes.get( i );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getHost(), node.getPort() + 1, files, false, null, qNode );
			}
		}
		
		// Send the update state to the client.
		MessageResponse message = new MessageResponse( (byte) ((clock != null) ? 0x1 : 0x0) );
        if(clock != null)
            message.addObject( DFSUtils.serializeObject( clock ) );
        session.sendMessage( message, true );
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
		// Send the message to the replica nodes.
		List<TCPSession> openSessions = new ArrayList<>();
		LOGGER.info( "Send request to replica nodes..." );
		
		int errNodes = 0;
		for(QuorumNode qNode : agreedNodes) {
			try{
				GossipMember node = qNode.getNode();
				TCPSession session = _net.tryConnect( node.getHost(), node.getPort(), 2000 );
				if(session != null) {
				    MessageRequest msg = new MessageRequest( Message.GET, fileName, DFSUtils.longToByteArray( qNode.getId() ) );
					session.sendMessage( msg, true );
					openSessions.add( session );
				}
				else {
				    if(!QuorumSession.unmakeQuorum( ++errNodes, Message.GET ))
				        return null;
				}
			} catch( IOException e ) {
			    if(!QuorumSession.unmakeQuorum( ++errNodes, Message.GET ))
                    return null;
			}
		}
		
		return openSessions;
	}
	
	/**
	 * Opens a direct connection with client.
	 * 
	 * @param clientAddress    the address where the connection is oriented
	*/
	private void openClientConnection( final String clientAddress ) throws IOException
	{
	    if(clientAddress != null) {
	        // If the address is not null means that it's a LoadBalancer address.
	        session.close();
    		
    		LOGGER.info( "Open a direct connection with the client: " + clientAddress );
    		String[] host = clientAddress.split( ":" );
    		session = _net.tryConnect( host[0], Integer.parseInt( host[1] ), 5000 );
	    }
	}
	
	/**
	 * Closes the resources opened by the instance node.
	*/
	public void closeWorker()
	{
		if(qSession != null)
		    qSession.closeQuorum();
		session.close();
		stats.decreaseValue( NodeStatistics.NUM_CONNECTIONS );
		
		completed = true;
		
		LOGGER.info( "[SN] Closed connection from: " + session.getSrcAddress() );
	}
	
	/**
	 * Start a thread, replacing an inactive one.
	 * 
	 * @param threadPool
	 * @param state
	*/
	public static DFSNode startThread( final ExecutorService threadPool, final ThreadState state ) throws IOException, JSONException
	{
		StorageNode node =
		        new StorageNode( state.getId(),
		                         true,
		                         null,// TODO per adesso e' null, ma poi metterci quello corretto
		                         state.getFileManager(),
		                         state.getQuorumThread(),
		                         state.getHashing(),
		                         state.getNet(),
		                         state.getSession(),
		                         state.getNetMonitor() );
		
		synchronized( threadPool ) {
			if(threadPool.isShutdown())
	            return null;
			
			threadPool.execute( node );
		}
		
		return node;
	}
	
	public static void main( String args[] ) throws Exception
	{
		ArgumentsParser.parseArgs( args, GossipMember.STORAGE );
		
		String ipAddress = ArgumentsParser.getIpAddress();
		List<GossipMember> members = ArgumentsParser.getNodes();
		String resourceLocation = ArgumentsParser.getResourceLocation();
		String databaseLocation = ArgumentsParser.getDatabaseLocation();
		
		StorageNode node = new StorageNode( ipAddress, members, resourceLocation, databaseLocation );
		node.launch();
	}
}