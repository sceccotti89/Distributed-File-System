/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.manager.NetworkMonitorSenderThread;
import distributed_fs.net.manager.NetworkMonitorThread;
import distributed_fs.net.manager.NodeStatistics;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.overlay.manager.FileTransferThread;
import distributed_fs.overlay.manager.MembershipManagerThread;
import distributed_fs.overlay.manager.QuorumThread;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.overlay.manager.ThreadMonitor;
import distributed_fs.overlay.manager.ThreadMonitor.ThreadState;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.VersioningUtils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;
import gossiping.GossipNode;

public class StorageNode extends DFSNode
{
	private GossipMember me;
	private QuorumThread quorum_t;
	private String resourcesLocation;
	private String databaseLocation;
	
	private List<Thread> threadsList;
	private MembershipManagerThread lMgr_t;
	
	// ===== Used by the node instance ===== //
	private TCPSession session;
	private String destId; // Virtual destination node identifier, for an input request.
	private List<QuorumNode> agreedNodes; // List of nodes that have agreed to the quorum.
	private boolean replacedThread;
	// =========================================== //
	
	
	
	
	/**
	 * Constructor with the default settings.<br>
	 * If you can't provide a configuration file,
	 * the list of nodes should be passed as arguments.<br>
	 * The node starts only using the {@link #launch(boolean)}
	 * method.
	 * 
	 * @param address				the ip address. If {@code null} it will be taken using the configuration file parameters.
	 * @param port                  the port number. If <= 0 the default one is assigned.
	 * @param virtualNodes          number of virtual nodes. If > 0 this value will be considered as static and never change,
	 *                              otherwise it's keep has the logarithm of the number of nodes present in the system.
	 * @param startupMembers		list of nodes
	 * @param resourcesLocation		the root where the resources are taken from.
	 * 								If {@code null} the default one will be selected ({@link distributed_fs.storage.DFSDatabase#RESOURCES_LOCATION});
	 * @param databaseLocation		the root where the database is located.
	 * 								If {@code null} the default one will be selected ({@link distributed_fs.storage.DFSDatabase#DATABASE_LOCATION});
	*/
	public StorageNode( final String address,
	                    final int port,
	                    final int virtualNodes,
	                    final List<GossipMember> startupMembers,
	                    final String resourcesLocation,
	                    final String databaseLocation ) throws IOException, InterruptedException, DFSException
	{
		super( address, port, virtualNodes, GossipMember.STORAGE, startupMembers );
		setName( "StorageNode" );
		
		me = gManager.getMyself();
		me.setId( DFSUtils.getNodeId( 1, me.getAddress() ) );
		lMgr_t = new MembershipManagerThread( _address, this.port, me, gManager );
		
		// Set the id to the remote nodes.
		List<GossipNode> nodes = gManager.getMemberList();
		for(GossipNode node : nodes) {
		    GossipMember member = node.getMember();
		    member.setId( DFSUtils.getNodeId( 1, member.getAddress() ) );
		}
		
		if(startupMembers != null) {
            for(GossipMember member : startupMembers) {
                if(member.getVirtualNodes() > 0 &&
                   member.getNodeType() != GossipMember.LOAD_BALANCER)
                    cHasher.addBucket( member, member.getVirtualNodes() );
            }
        }
		
		cHasher.addBucket( me, me.getVirtualNodes() );
		quorum_t = new QuorumThread( this.port, _address, this );
		
		fMgr = new FileTransferThread( me, this.port, cHasher, quorum_t, resourcesLocation, databaseLocation );
		
		netMonitor = new NetworkMonitorSenderThread( _address, this );
		
		this.resourcesLocation = resourcesLocation;
        this.databaseLocation = databaseLocation;
		
		threadsList = new ArrayList<>( MAX_USERS );
		monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, this.port, MAX_USERS );
		monitor_t.addElements( me, quorum_t, cHasher, resourcesLocation, databaseLocation );
	
		this.port += PORT_OFFSET;
	}
	
	/**
	 * Load the StorageNode from the configuration file.
	 * 
	 * @param configFile   the configuration file
	*/
	public static StorageNode fromJSONFile( final JSONObject configFile ) throws IOException, InterruptedException, DFSException
	{
	    String address = configFile.has( "Address" ) ? configFile.getString( "Address" ) : null;
	    int port = configFile.has( "Port" ) ? configFile.getInt( "Port" ) : 0;
	    int vNodes = configFile.has( "vNodes" ) ? configFile.getInt( "vNodes" ) : 0;
	    String resourcesLocation = configFile.has( "ResourcesLocation" ) ? configFile.getString( "ResourcesLocation" ) : null;
	    String databaseLocation = configFile.has( "DatabaseLocation" ) ? configFile.getString( "DatabaseLocation" ) : null;
	    List<GossipMember> members = getStartupMembers( configFile );
	    
	    return new StorageNode( address, port, vNodes, members, resourcesLocation, databaseLocation );
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
	    fMgr.setAntiEntropy( enable );
	}
	
	/**
	 * Starts the node.<br>
	 * It can be launched in an asynchronous way, creating a new Thread that
	 * runs this process.
	 * 
	 * @param launchAsynch   {@code true} to launch the process asynchronously, {@code false} otherwise
	*/
	public void launch( final boolean launchAsynch )
	{
	    if(launchAsynch) {
	        // Create a new Thread.
	        Thread t = new Thread() {
	            @Override
	            public void run() {
	                startProcess();
	            }
	        };
	        t.setName( "StorageNode" );
	        t.setDaemon( true );
	        t.start();
	    }
	    else {
	        startProcess();
	    }
	}
	
	private void startProcess()
	{
	    fMgr.start();
		netMonitor.start();
		quorum_t.start();
		monitor_t.start();
		lMgr_t.start();
		
		if(startGossiping)
		    gManager.start();
		
		LOGGER.info( "[SN] Waiting on: " + _address + ":" + port );
		
		try {
			while(!shutDown.get()) {
				TCPSession session = _net.waitForConnection( _address, port );
				if(session != null) {
				    synchronized( threadPool ) {
				        if(threadPool.isShutdown())
				            break;
				        
				        StorageNode node = new StorageNode( getNextThreadID(), false, fMgr,
				                                            quorum_t, cHasher, _net, session, netMonitor );
				        monitor_t.addThread( node );
				        threadPool.execute( node );
				    }
				}
				
			    // Check whether the monitor thread is alive: if not a new instance is activated.
			    if(!shutDown.get() && !monitor_t.isAlive()) {
			        monitor_t = new ThreadMonitor( this, threadPool, threadsList, _address, port, MAX_USERS );
			        monitor_t.addElements( me, quorum_t, cHasher, resourcesLocation, databaseLocation );
			        monitor_t.start();
			    }
			}
		}
		catch( IOException e ) {
		    if(!shutDown.get()) {
                e.printStackTrace();
                close();
            }
		}
		
		LOGGER.info( "[SN] '" + _address + ":" + port + "' Closed." );
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
                         final FileTransferThread fMgr,
                         final QuorumThread quorum_t,
                         final ConsistentHasher<GossipMember, String> cHasher,
                         final TCPnet net,
                         final TCPSession session,
                         final NetworkMonitorThread netMonitor ) throws IOException
    {
    	super( net, fMgr, cHasher );
    	setName( "StorageNode" );
    	setId( id );
    	
    	this.replacedThread = replacedThread;
    	this.quorum_t = quorum_t;
    	this.session = session;
    	this.netMonitor = netMonitor;
    	
    	actionsList = new ArrayDeque<>( 32 );
    	state = new ThreadState( id, replacedThread, actionsList,
    	                         fMgr, quorum_t, cHasher, this.netMonitor );
    }

    @Override
	public void run()
	{
		LOGGER.info( "[SN] Received a connection from: " + session.getEndPointAddress() );
		if(!actionsList.isEmpty())
		    actionsList.removeFirst();
		else {
		    stats.increaseValue( NodeStatistics.NUM_CONNECTIONS );
		    actionsList.addLast( DONE );
		}
		
		try {
		    MessageRequest data = state.getValue( ThreadState.NEW_MSG_REQUEST );
		    if(data == null) {
		        data = session.receiveMessage();
		        state.setValue( ThreadState.NEW_MSG_REQUEST, data );
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
				agreedNodes = quorum_t.checkQuorum( state, session, opType, fileName, destId );
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
						quorum_t.sendQuorumResponse( state, session, Message.TRANSACTION_OK );
					}
				}
			}
			
			switch( opType ) {
				case( Message.PUT ):
				    DistributedFile file = new DistributedFile( data.getPayload() );
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
				    DistributedFile dFile = new DistributedFile( data.getPayload() );
				    // Get (if present) the hinted handoff address.
                    hintedHandoff = meta.getHintedHandoff();
                    LOGGER.debug( "[SN] DELETE: " + dFile.getName() + ":" + hintedHandoff );
				    
				    handleDELETE( isCoordinator, dFile, hintedHandoff );
				    break;
			}
		}
		catch( IOException e ) {
			e.printStackTrace();
		}
		
		closeWorker();
	}
	
	private void handlePUT( final boolean isCoordinator, final DistributedFile file, final String hintedHandoff ) throws IOException
	{
		VectorClock clock;
		if(!replacedThread || actionsList.isEmpty()) {
		    clock = file.getVersion().incremented( _address );
		    clock = fMgr.getDatabase().saveFile( file, clock, hintedHandoff, true );
		    state.setValue( ThreadState.UPDATE_CLOCK_DB, clock );
		    actionsList.addLast( DONE );
		}
		else {
		    clock = state.getValue( ThreadState.UPDATE_CLOCK_DB );
		    actionsList.removeFirst();
		}
		LOGGER.debug( "[SN] UPDATED: " + (clock != null) + ", FILE: " + fMgr.getDatabase().getFile( file.getName() ) );
		
		if(clock == null) // Not updated.
			quorum_t.closeQuorum( state, agreedNodes );
		else {
		    file.setVersion( clock );
		    
			// Send, in parallel, the file to the replica nodes.
			List<DistributedFile> files = Collections.singletonList( file );
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				QuorumNode qNode = agreedNodes.get( i );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getHost(), node.getPort(), files, false, null, qNode );
			}
		}
		
		sendClientResponse( clock );
	}
	
	private void handleGET( final boolean isCoordinator, final String fileName, final long fileId ) throws IOException
	{
		if(isCoordinator) {
			// Send the GET request to all the agreed nodes,
			// to retrieve their version of the file and make the reconciliation.
			List<TCPSession> openSessions = sendRequestToReplicaNodes( fileName );
			if(openSessions == null) {
			    quorum_t.sendQuorumResponse( state, session, Message.TRANSACTION_FAILED );
			    return;
			}
			
			// The replica files can be less than the quorum.
			LOGGER.info( "Receive the files from the replica nodes..." );
			List<DistributedFile> filesToSend = state.getValue( ThreadState.FILES_TO_SEND );
			if(filesToSend == null) {
			    filesToSend = new ArrayList<>( QuorumSession.getMaxNodes() + 1 ); // +1 for the own copy.
			    state.setValue( ThreadState.FILES_TO_SEND, filesToSend );
			}
			
			getReplicaVersions( filesToSend, openSessions );
			
			// Send the positive notification to the client.
			LOGGER.info( "[SN] Quorum completed successfully: " + openSessions.size() + "/" + QuorumSession.getMinQuorum( Message.GET ) );
			quorum_t.sendQuorumResponse( state, session, Message.TRANSACTION_OK );
			
			if(!replacedThread || actionsList.isEmpty()) {
    			// Put in the list the file present in the database of this node.
    			DistributedFile dFile = fMgr.getDatabase().getFile( fileName );
    			if(dFile != null) {
    			    dFile.loadContent( fMgr.getDatabase() );
    				filesToSend.add( dFile );
    			}
    			
    			// Try a first reconciliation.
    			LOGGER.debug( "Files: " + filesToSend.size() );
    			List<DistributedFile> reconciledFiles = makeReconciliation( filesToSend );
    			LOGGER.debug( "Files after reconciliation: " + reconciledFiles.size() );
    			
    			// Send the files directly to the client.
    			int size = reconciledFiles.size();
                session.sendMessage( DFSUtils.intToByteArray( size ), false );
                
                for(int i = 0; i < size; i++) {
                    LOGGER.debug( "Sending file \"" + dFile + "\"" );
                    session.sendMessage( reconciledFiles.get( i ).read(), true );
                    LOGGER.debug( "File \"" + reconciledFiles.get( i ).getName() + "\" sent." );
                }
    			
			    
			    actionsList.addLast( DONE );
			    LOGGER.info( "Files sent to the client." );
			}
			else
			    actionsList.removeFirst();
		}
		else {
			// REPLICA node: send the requested file to the coordinator.
		    DistributedFile file = fMgr.getDatabase().getFile( fileName );
		    byte[] message;
		    if(file == null)
				message = new byte[]{ (byte) 0x0 };
			else {
			    file.loadContent( fMgr.getDatabase() );
			    message = _net.createMessage( new byte[]{ (byte) 0x1 }, file.read(), true );
			}
			
		    // Remove the lock to the file.
		    if(!replacedThread || actionsList.isEmpty()) {
		        quorum_t.setLocked( false, fileName, fileId, Message.GET );
		        actionsList.addLast( DONE );
            }
            else
                actionsList.removeFirst();
			
			if(!replacedThread || actionsList.isEmpty()) {
			    session.sendMessage( message, true );
			    actionsList.addLast( DONE );
			}
			else
			    actionsList.removeFirst();
		}
	}
	
	/** 
     * Sends the actual request to the replica nodes.
     * 
     * @param fileName		name of the file to send
     * 
     * @return list of sessions opened with other replica nodes.
    */
    private List<TCPSession> sendRequestToReplicaNodes( final String fileName )
    {
    	List<TCPSession> openSessions = state.getValue( ThreadState.OPENED_SESSIONS );
    	if(openSessions == null) {
    	    openSessions = new ArrayList<>( QuorumSession.getMaxNodes() );
    	    state.setValue( ThreadState.OPENED_SESSIONS, openSessions );
    	}
    	
    	LOGGER.info( "Send request to replica nodes..." );
    	int errNodes = 0;
    	for(QuorumNode qNode : agreedNodes) {
    		try{
    			GossipMember node = qNode.getNode();
    			TCPSession session;
    			// Get the remote connection.
    			if(!replacedThread || actionsList.isEmpty()) {
    			    session = _net.tryConnect( node.getHost(), node.getPort() + PORT_OFFSET, 2000 );
    			    state.setValue( ThreadState.REPLICA_REQUEST_CONN, session );
    			    actionsList.addLast( DONE );
    			}
    			else {
    			    session = state.getValue( ThreadState.REPLICA_REQUEST_CONN );
    			    actionsList.removeFirst();
    			}
    			
    			// Check the response (if any).
    			if(session != null) {
    			    if(!replacedThread || actionsList.isEmpty()) {
    				    MessageRequest msg = new MessageRequest( Message.GET, fileName, DFSUtils.longToByteArray( qNode.getId() ) );
    					session.sendMessage( msg, true );
    					openSessions.add( session );
    					actionsList.add( DONE );
    			    }
    			    else
    			        actionsList.removeFirst();
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
	 * Gets the replica versions.
	 * 
	 * @param filesToSend      
	 * @param openSessions     
	*/
	private void getReplicaVersions( final List<DistributedFile> filesToSend,
	                                 final List<TCPSession> openSessions ) throws IOException
	{
	    // Get the value of the indexes.
        Integer errors = state.getValue( ThreadState.QUORUM_ERRORS );
        if(errors == null) errors = 0;
        Integer offset = state.getValue( ThreadState.QUORUM_OFFSET );
        if(offset == null) offset = 0;
        Integer toIndex = state.getValue( ThreadState.NODES_INDEX );
        if(toIndex == null) toIndex = 0; toIndex = toIndex - offset;
        int index = 0;
        
        // Get the replica versions.
        for(TCPSession session : openSessions) {
            if(index == toIndex) {
                try{
                    ByteBuffer data;
                    if(!replacedThread || actionsList.isEmpty()) {
                        data = ByteBuffer.wrap( session.receive() );
                        state.setValue( ThreadState.REPLICA_FILE, data );
                        actionsList.addLast( DONE );
                    }
                    else {
                        data = state.getValue( ThreadState.REPLICA_FILE );
                        actionsList.removeFirst();
                    }
                    
                    if(!replacedThread || actionsList.isEmpty()) {
                        if(data.get() == (byte) 0x1) {
                            // Replica node owns the requested file.
                            byte[] file = DFSUtils.getNextBytes( data );
                            filesToSend.add( new DistributedFile( file ) );
                            
                            // Update the list of agreedNodes.
                            agreedNodes.remove( index - offset );
                            state.setValue( ThreadState.QUORUM_OFFSET, ++offset );
                        }
                        actionsList.add( DONE );
                    }
                    else
                        actionsList.removeFirst();
                }
                catch( IOException e ) {
                    if(QuorumSession.unmakeQuorum( ++errors, Message.GET )) {
                        LOGGER.info( "[SN] Quorum failed: " + openSessions.size() + "/" + QuorumSession.getMinQuorum( Message.GET ) );
                        quorum_t.cancelQuorum( state, this.session, agreedNodes );
                        return;
                    }
                    state.setValue( ThreadState.QUORUM_ERRORS, errors );
                }
                
                state.setValue( ThreadState.NODES_INDEX, ++toIndex );
            }
            
            index++;
            session.close();
        }
	}
	
	/**
	 * Makes the reconciliation among different vector clocks.
	 * 
	 * @param files		list of files to compare
	 * 
	 * @return The list of uncorrelated versions.
	*/
	private List<DistributedFile> makeReconciliation( final List<DistributedFile> files )
	{
		List<Versioned<DistributedFile>> versions = new ArrayList<>();
		for(DistributedFile file : files)
			versions.add( new Versioned<DistributedFile>( file, file.getVersion() ) );
		
		// Resolve the versions..
		List<Versioned<DistributedFile>> inconsistency = VersioningUtils.resolveVersions( versions );
		
		// Get the uncorrelated files.
		List<DistributedFile> uncorrelatedVersions = new ArrayList<>();
		for(Versioned<DistributedFile> version : inconsistency)
			uncorrelatedVersions.add( version.getValue() );
		
		return uncorrelatedVersions;
	}
	
	private void handleGET_ALL( final String clientAddress ) throws IOException
    {
        openClientConnection( clientAddress );
        
        if(!replacedThread || actionsList.isEmpty()) {
            List<DistributedFile> files = fMgr.getDatabase().getAllFiles();
            int size = files.size();
            session.sendMessage( DFSUtils.intToByteArray( size ), false );
            
            for(int i = 0; i < size; i++) {
                DistributedFile file = files.get( i );
                file.loadContent( fMgr.getDatabase() );
                
                LOGGER.debug( "Sending file \"" + file + "\"" );
                session.sendMessage( file.read(), true );
                LOGGER.debug( "File \"" + file.getName() + "\" transmitted." );
            }
            
            actionsList.addLast( DONE );
        }
    }
	
	private void handleDELETE( final boolean isCoordinator, final DistributedFile file, final String hintedHandoff ) throws IOException
	{
		VectorClock clock;
		if(!replacedThread || actionsList.isEmpty()) {
    		clock = file.getVersion().incremented( _address );
    		//System.out.println( "IN: " + clock + ", MY: " + fMgr.getDatabase().getFile( file.getName() ).getVersion() );
    		clock = fMgr.getDatabase().deleteFile( file.getName(), clock, file.isDirectory(), hintedHandoff );
    		state.setValue( ThreadState.UPDATE_CLOCK_DB, clock );
            actionsList.addLast( DONE );
        }
        else {
            clock = state.getValue( ThreadState.UPDATE_CLOCK_DB );
            actionsList.removeFirst();
        }
		LOGGER.debug( "[SN] Updated: " + (clock != null) );
		
		if(clock == null)
			quorum_t.closeQuorum( state, agreedNodes );
		else {
		    LOGGER.debug( "[SN] Deleted file \"" + file.getName() + "\"" );
			file.setVersion( clock );
			file.setDeleted( true );
			
			// Send, in parallel, the deleted node to all the agreed nodes.
			List<DistributedFile> files = Collections.singletonList( file );
			for(int i = agreedNodes.size() - 1; i >= 0; i--) {
				QuorumNode qNode = agreedNodes.get( i );
				GossipMember node = qNode.getNode();
				fMgr.sendFiles( node.getHost(), node.getPort(), files, false, null, qNode );
			}
		}
		
		sendClientResponse( clock );
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
            if(!replacedThread || actionsList.isEmpty()) {
    	        session.close();
        		
        		LOGGER.info( "Open a direct connection with the client: " + clientAddress );
        		String[] host = clientAddress.split( ":" );
        		session = _net.tryConnect( host[0], Integer.parseInt( host[1] ), 5000 );
        		
        		actionsList.addLast( DONE );
            }
        }
    }

    /** 
	 * Sends the update state to the client.
	 * 
	 * @param clock    the updated clock to send
	*/
	private void sendClientResponse( final VectorClock clock ) throws IOException
	{
	    if(!replacedThread || actionsList.isEmpty()) {
            LOGGER.debug( "[SN] Sending the updated clock to the client..." );
            MessageResponse message;
            if(clock == null)
                message = new MessageResponse( (byte) 0x0 );
            else {
                message = new MessageResponse( (byte) 0x1 );
                //message.addObject( DFSUtils.serializeObject( clock ) );
            }
            session.sendMessage( message, true );
            LOGGER.debug( "[SN] Clock sent." );
            actionsList.addLast( DONE );
        }
        else
            actionsList.removeFirst();
	}
	
	/**
	 * Closes the resources opened by the instance node.
	*/
	public void closeWorker()
	{
		session.close();
		
		if(!replacedThread || actionsList.isEmpty()) {
		    stats.decreaseValue( NodeStatistics.NUM_CONNECTIONS );
		    completed = true;
		    actionsList.addLast( DONE );
		}
		else
		    actionsList.removeFirst();
		
		LOGGER.info( "[SN] Closed connection from: " + session.getEndPointAddress() );
	}
	
	/**
	 * Start a thread, replacing an inactive one.
	 * 
	 * @param threadPool   the pool of threads
	 * @param state        the state of the dead Thread
	*/
	public static DFSNode startThread( final ExecutorService threadPool, final ThreadState state ) throws IOException
	{
		StorageNode node =
		        new StorageNode( state.getId(), true,
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
	
	@Override
	public void close()
	{
	    if(shutDown.get())
	        return;
	    
	    super.close();
	    
	    quorum_t.close();
	    lMgr_t.close();
	    
	    try {
	        quorum_t.join();
	        lMgr_t.join();
	    }
	    catch( InterruptedException e ) {}
	}
}