/**
 * @author Stefano Ceccotti
*/

package client;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

import client.manager.ClientSynchronizer;
import client.manager.DFSManager;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.storage.DBManager.DBListener;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.Occurred;
import distributed_fs.versioning.VectorClock;
import gossiping.GossipMember;

/**
 * Starts the service for a client user.
 * All the methods are provided by the {@link IDFSService}
 * interface, used to put, get and delete files.
 * There is also the possibility to get a list of all the files
 * stored in a random remote node.<br>
 * Client synchronization and membership operations
 * are performed in background, where the latter only
 * with the {@link distributed_fs.overlay.LoadBalancer LoadBalancer} mode disabled.
 * The synchronization Thread can be disabled too.
*/
public class DFSService extends DFSManager implements IDFSService
{
	private final Random random;
	private final DFSDatabase database;
	private ClientSynchronizer syncClient;
	private String hintedHandoff = null;
	
	private boolean disableSyncThread = false;
	
	private boolean testing = false;
	private boolean initialized = false;
	
	private final ReentrantLock lock = new ReentrantLock( true );
	
	
	
	public DFSService( final String ipAddress,
					   final int port,
					   final boolean useLoadBalancer,
					   final List<GossipMember> members,
					   final String resourcesLocation,
					   final String databaseLocation,
					   final DBListener listener ) throws IOException, DFSException
	{
		super( ipAddress, port, useLoadBalancer, members );
		
		random = new Random();
		database = new DFSDatabase( resourcesLocation, databaseLocation, null );
		if(listener != null)
		    database.addListener( listener );
		
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
			    shutDown();
				LOGGER.info( "Service has been shutdown..." );
			}
		});
	}
	
	/**
	 * Disable the synchronization thread.
	 * 
	 * @return this builder
	*/
	public DFSService disableSyncThread() {
		disableSyncThread = true;
		return this;
	}
	
	/**
	 * Starts the service.
	 * 
	 * @return {@code true} if the service has been successfully started,
	 * 		   {@code false} otherwise.
	*/
	public boolean start() throws IOException
	{
		if(!disableSyncThread) {
		    syncClient = new ClientSynchronizer( this, database, lock );
		    syncClient.start();
		}
	    
	    if(!useLoadBalancer)
	        membMgr_t.start();
		
		LOGGER.info( "System up." );
		return (initialized = true);
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	 * @throws InterruptedException 
	*/
	public DistributedFile getFile( final String fileName )
	{
		DistributedFile file = database.getFile( fileName );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Realoads the database, forcing it to
	 * checks if some brand spanking new file is present.
	*/
	public void reloadDB() throws IOException
	{
	    database.loadFiles();
	}
	
	@Override
	public List<DistributedFile> getAllFiles() throws DFSException
	{
	    if(isClosed()) {
            LOGGER.error( "Sorry but the service is closed." );
            return null;
        }
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		List<DistributedFile> files = null;
		boolean completed = true;
		
		// Here the name of the file is not important.
		final String fileName = "";
		TCPSession session = null;
		if((session = contactRemoteNode( fileName, Message.GET_ALL )) == null)
		    return null;
		
		try{
			//LOGGER.debug( "Sending message..." );
			sendGetAllMessage( session, fileName );
			//LOGGER.debug( "Request message sent" );
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if(!checkResponse( session, "GET_ALL", true ))
	                throw new IOException();
			    
			    if((session = waitRemoteConnection( session )) == null)
                    throw new IOException();
            }
			
			files = readGetAllResponse( session );
		}
		catch( IOException e ) {
		    //e.printStackTrace();
		    completed = false;
		}
		
		session.close();
		if(completed)
		    LOGGER.info( "Operation GET_ALL completed." );
		
		return files;
	}
	
	@Override
	public DistributedFile get( final String fileName ) throws DFSException
	{
	    if(isClosed()) {
            LOGGER.error( "Sorry but the service is closed." );
            return null;
        }
	    
	    Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		
		if(!initialized) {
		    throw new DFSException( "The system has not been initialized.\n" +
		                            "Use the \"start\" method to initialize the system." );
		}
		
		LOGGER.info( "Starting GET operation: " + fileName );
		DistributedFile backToClient;
		boolean unlock = false;
		
		String normFileName = database.normalizeFileName( fileName );
		TCPSession session = null;
		if((session = contactRemoteNode( normFileName, Message.GET )) == null)
			return null;
		
		try {
			// Send the request.
			//LOGGER.info( "Sending message..." );
			sendGetMessage( session, normFileName );
			//LOGGER.info( "Message sent" );
			
			// Checks whether the load balancer has found an available node,
			// or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "GET", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if((session = waitRemoteConnection( session )) == null ||
    			   !checkResponse( session, "GET", true ))
    				throw new IOException();
			}
			
			// Receive one or more files.
			List<DistributedFile> files = readGetResponse( session );
			
			int size = files.size();
			if(size == 0) {
				LOGGER.info( "File \"" + fileName + "\" not found." );
				session.close();
				return null;
			}
			
			//LOGGER.info( "Received " + files.size() + " files." );
			
			int id = 0;
			VectorClock clock = new VectorClock();
			//List<DistributedFile> versions = new ArrayList<>( size );
			// Generate only one clock, merging all the received versions.
			for(DistributedFile file : files) {
				//versions.add( new DistributedFile( file, null ) );
				clock = clock.merge( file.getVersion() );
			}
			
			lock.lock();
            unlock = true;
            
            boolean reconciled = false;
            backToClient = database.getFile( fileName );
            if(backToClient == null) {
                // Choose the version among the only received files.
                reconciled = false;
                id = syncClient.makeReconciliation( files );
            }
            else {
                if(backToClient.getVersion().compare( clock ) == Occurred.CONCURRENTLY) {
                    System.out.println( "MY: " + backToClient.getVersion() + ", IN: " + clock );
                    files.add( backToClient );
                    id = syncClient.makeReconciliation( files );
                    reconciled = true;
                }
            }
			
			// Update the database.
            DistributedFile file = files.get( id );
			if(file.isDeleted())
			    database.deleteFile( fileName, clock, file.isDirectory(), null );
			else
			    database.saveFile( file, clock, null, true );
			
			backToClient = database.getFile( fileName );
			
			unlock = false;
			lock.unlock();
			
			// Send back the reconciled version.
            if(size > 1 && reconciled) {
                // If the operation is not performed an exception is thrown.
                if((!file.isDeleted() && !put( fileName )) ||
                    (file.isDeleted() && !delete( fileName )))
                    throw new IOException();
            }
		}
		catch( IOException e ) {
			LOGGER.info( "Operation GET not performed. Try again later." );
			if(unlock)
			    lock.unlock();
			//e.printStackTrace();
			session.close();
			return null;
		}
		
		session.close();
		LOGGER.info( "Operation GET successfully completed. Received: " + backToClient );
		
		return backToClient;
	}
	
	@Override
	public boolean put( final String fileName ) throws DFSException, IOException
	{
	    if(isClosed()) {
            LOGGER.error( "Sorry but the service is closed." );
            return false;
        }
	    
	    Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		LOGGER.info( "Starting PUT operation: " + fileName );
		
		lock.lock();
		
		String dbRoot = database.getFileSystemRoot();
		String normFileName = database.normalizeFileName( fileName );
		if(!database.existFile( dbRoot + normFileName, false )) {
		    lock.unlock();
		    LOGGER.error( "Operation PUT not performed: file \"" + fileName + "\" not founded. " );
            LOGGER.error( "The file must be present in one of the sub-directories starting from: " + dbRoot );
            return false;
        }
		
		DistributedFile file = database.getFile( fileName );
		if(file == null) {
            File f = new File( dbRoot + normFileName );
            file = new DistributedFile( normFileName, f.isDirectory(), new VectorClock(), null );
        }
		
		TCPSession session = null;
        if((session = contactRemoteNode( normFileName, Message.PUT )) == null) {
            lock.unlock();
            return false;
        }
		
		boolean completed = true;
		
		try {
			//LOGGER.info( "Sending file..." );
			
			// Send the file.
		    file.setDeleted( false );
		    file.loadContent( database );
			sendPutMessage( session, file, hintedHandoff );
			//LOGGER.info( "File sent" );
			
			// Checks whether the load balancer has found an available node,
            // or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "PUT", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if((session = waitRemoteConnection( session )) == null ||
    			   !checkResponse( session, "PUT", true ))
    				throw new IOException();
			}
			
			// Update the file's vector clock.
			MessageResponse message = session.receiveMessage();
            if(message.getType() == (byte) 0x1) {
                LOGGER.debug( "Updating version of the file '" + fileName + "'..." );
                VectorClock newClock = file.getVersion().incremented( session.getEndPointAddress() );
                database.saveFile( file, newClock, null, false );
            }
		}
		catch( IOException e ) {
			//e.printStackTrace();
			LOGGER.info( "Operation PUT '" + fileName + "' not performed. Try again later." );
			completed = false;
		}
		
		lock.unlock();
		
		if(completed)
			LOGGER.info( "Operation PUT '" + fileName + "' successfully completed." );
		
		session.close();
		
		return completed;
	}
	
	@Override
	public boolean delete( final String fileName ) throws IOException, DFSException
	{
	    if(isClosed()) {
            LOGGER.error( "Sorry but the service is closed." );
	        return false;
	    }
	    
	    Preconditions.checkNotNull( fileName, "fileName cannot be null." );
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		boolean completed = true;
		
		lock.lock();
		
		String dbRoot = database.getFileSystemRoot();
        String normFileName = database.normalizeFileName( fileName );
        if(!database.existFile( dbRoot + normFileName, false )) {
            lock.unlock();
            LOGGER.error( "Operation DELETE for " + fileName + " not performed. File not found." );
            LOGGER.error( "The file must be present in one of the sub-directories starting from: " + dbRoot );
            return false;
        }
		
		/*if(file.isDirectory()) {
		    // Delete recursively all the files present in the directory and sub-directories.
		    File inputFile = new File( dbRoot + normFileName );
		    File[] files = inputFile.listFiles();
		    if(files != null) {
    			for(File f: files) {
    				LOGGER.debug( "Name: " + f.getPath() + ", Directory: " + f.isDirectory() );
    				if(!delete( f.getPath() ))
    					break;
    			}
		    }
		}*/
		
		LOGGER.info( "Starting DELETE operation for: " + fileName );
		
		DistributedFile file = database.getFile( fileName );
		if(file == null) {
		    File f = new File( dbRoot + normFileName );
            file = new DistributedFile( normFileName, f.isDirectory(), new VectorClock(), null );
		}
		
		TCPSession session = null;
		if((session = contactRemoteNode( normFileName, Message.DELETE )) == null) {
			lock.unlock();
		    return false;
		}
		
		try {
			sendDeleteMessage( session, file, hintedHandoff );
			
			// Checks whether the load balancer has found an available node,
            // or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "DELETE", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if((session = waitRemoteConnection( session )) == null ||
    			   !checkResponse( session, "DELETE", true ))
    				throw new IOException();
			}
			
			// Update the file's vector clock.
			MessageResponse message = session.receiveMessage();
			LOGGER.debug( "Updating file: " + (message.getType() == (byte) 0x1) );
			if(message.getType() == (byte) 0x1) {
			    LOGGER.debug( "Updating version of the file '" + fileName + "'..." );
			    VectorClock newClock = file.getVersion().incremented( session.getEndPointAddress() );
    			database.deleteFile( fileName, newClock, file.isDirectory(), null );
			}
		}
		catch( IOException e ) {
			e.printStackTrace();
			LOGGER.info( "Operation DELETE for \"" + fileName + "\" not performed. " +
			             "Try again later." );
			completed = false;
		}
		
		lock.unlock();
		
		if(completed)
		    LOGGER.info( "DELETE operation for \"" + fileName + "\" completed successfully." );
		session.close();
		
		return completed;
	}
	
	@Override
	public List<DistributedFile> listFiles()
	{
		return database.getAllFiles();
	}
	
	/**
     * Tries a connection with the first available remote node.
     * 
     * @param fileName    name of the file
     * @param opType      operation type
     * 
     * @return the TCP session if at least one remote node is available,
     *         {@code null} otherwise.
    */
	private TCPSession contactRemoteNode( final String fileName, final byte opType )
	{
	    TCPSession session;
	    
	    if(useLoadBalancer && (session = contactLoadBalancerNode( opType )) != null)
	        return session;
	    
	    if(!useLoadBalancer && (session = contactStorageNode( fileName, opType )) != null)
	        return session;
	    
	    return null;
	}
	
	/**
	 * Tries a connection with the first available LoadBalancer node.
	 * 
	 * @param opType   type of the operation
	 * 
	 * @return the TCP session if at least one remote node is available,
	 * 		   {@code false} otherwise.
	*/
	private TCPSession contactLoadBalancerNode( final byte opType )
	{
	    if(opType != Message.GET_ALL)
	        LOGGER.info( "Contacting a balancer node..." );
		
		TCPSession session = null;
		HashSet<String> filterAddress = new HashSet<>();
		List<GossipMember> nodes = new ArrayList<>( loadBalancers );
		
		while(session == null) {
			if(filterAddress.size() == loadBalancers.size() || nodes.size() == 0)
				break;
			
			GossipMember partner = selectNode( nodes );
			if(filterAddress.contains( partner.getHost() ))
				nodes.remove( partner );
			else {
				filterAddress.add( partner.getHost() );
				if(opType != Message.GET_ALL)
				    LOGGER.info( "Contacting " + partner + "..." );
				try{ session = net.tryConnect( partner.getHost(), partner.getPort() + DFSNode.PORT_OFFSET, 2000 ); }
				catch( IOException e ) {
					//e.printStackTrace();
					nodes.remove( partner );
					System.out.println( "Node " + partner + " unreachable." );
				}
			}
		}
		
		if(session == null) {
		    if(opType != Message.GET_ALL)
		        LOGGER.error( "Sorry, but the service is not available. Retry later." );
			return null;
		}
		
		return session;
	}
	
	/**
     * Tries a connection with the first available StorageNode node.
     * 
     * @param fileName    name of the file
     * @param opType      operation type
     * 
     * @return the TCP session if at least one remote node is available,
     *         {@code null} otherwise.
    */
    private TCPSession contactStorageNode( final String fileName, final byte opType )
    {
        if(opType != Message.GET_ALL)
            LOGGER.info( "Contacting a storage node..." );
        
        String fileId = DFSUtils.getId( fileName );
        TCPSession session = null;
        List<GossipMember> nodes = null;
        synchronized( cHasher ) {
            String nodeId = cHasher.getNextBucket( fileId );
            if(nodeId == null) return null;
            GossipMember node = cHasher.getBucket( nodeId );
            if(node == null) return null;
            nodes = getNodesFromPreferenceList( nodeId, node );
        }
        
        boolean nodeDown = false;
        if(nodes != null) {
            hintedHandoff = null;
            for(GossipMember member : nodes) {
                if(opType != Message.GET_ALL)
                    LOGGER.debug( "[CLIENT] Contacting: " + member );
                try {
                    session = net.tryConnect( member.getHost(), member.getPort() + DFSNode.PORT_OFFSET, 2000 );
                    destId = member.getId();
                    return session;
                } catch( IOException e ) {
                    // Ignored.
                    //e.printStackTrace();
                    if(!nodeDown)
                        nodeDown = true;
                    
                    if((opType == Message.PUT || opType == Message.DELETE) &&
                       hintedHandoff == null)
                        hintedHandoff = member.getHost() + ":" + member.getPort();
                }
            }
        }
        
        // Start now the background thread
        // for the membership.
        if(nodeDown && membMgr_t.isAlive())
            membMgr_t.wakeUp();
        
        if(opType != Message.GET_ALL)
            LOGGER.error( "Sorry, but the service is not available. Retry later." );
        return null;
    }
    
    /**
     * Gets the first N nodes from the node's preference list,
     * represented by its identifier.<br>
     * For simplicity, its preference list is made by nodes
     * encountered while walking the DHT.
     * 
     * @param id            the input node identifier
     * @param sourceNode    the source node that have started the procedure
     * 
     * @return list of nodes taken from the given node's preference list.
    */
    private List<GossipMember> getNodesFromPreferenceList( final String id, final GossipMember sourceNode )
    {
        final int PREFERENCE_LIST = QuorumSession.getMaxNodes();
        List<GossipMember> nodes = getSuccessorNodes( id, sourceNode.getAddress(), PREFERENCE_LIST );
        nodes.add( sourceNode );
        return nodes;
    }
    
    /**
     * Returns the successor nodes of the input id.
     * 
     * @param id                source node identifier
     * @param addressToRemove   the address to skip during the procedure
     * @param numNodes          number of requested nodes
     * 
     * @return the list of successor nodes;
     *         it could contains less than {@code numNodes} elements.
    */
    private List<GossipMember> getSuccessorNodes( final String id, final String addressToRemove, final int numNodes )
    {
        List<GossipMember> nodes = new ArrayList<>( numNodes );
        Set<String> filterAddress = new HashSet<>();
        int size = 0;
        
        filterAddress.add( addressToRemove );
        
        // Choose the nodes whose address is different than the input node.
        String currId = id, succ;
        while(size < numNodes) {
            succ = cHasher.getNextBucket( currId );
            if(succ == null || succ.equals( id ))
                break;
            
            GossipMember node = cHasher.getBucket( succ );
            if(node != null) {
                currId = succ;
                if(!filterAddress.contains( node.getAddress() )) {
                    nodes.add( node );
                    filterAddress.add( node.getAddress() );
                    size++;
                }
            }
        }
        
        return nodes;
    }

    /**
	 * Wait the incoming connection from the StorageNode.
	 * 
	 * @param session  the current TCP session
	 * 
	 * @return the TCP session if the connection if it has been established,
	 * 		   {@code null} otherwise
	*/
	private TCPSession waitRemoteConnection( final TCPSession session ) throws IOException
	{
	    session.close();
		LOGGER.info( "Wait the incoming connection..." );
		return net.waitForConnection();
	}
	
	/**
	 * Find a random node from the local membership list.
	 * The node is guaranteed to have been chosen uniformly.
	 *
	 * @return a random member
	 */
	private <T> T selectNode( final List<T> nodes )
	{
		int randomNeighborIndex = random.nextInt( nodes.size() );
		return nodes.get( randomNeighborIndex );
	}
	
	/**
	 * Returns the state of the system.
	 * 
	 * @return {@code true} if the system is down,
	 * 		   {@code false} otherwise.
	*/
	public boolean isClosed()
	{
		return closed.get();
	}
	
	@Override
	public void shutDown()
	{
	    super.shutDown();
	    
	    if(!disableSyncThread)
	        syncClient.shutDown();
		
		//if(session != null && !session.isClosed())
			//session.close();
		net.close();
		
		if(!testing)
			database.close();
		
		LOGGER.info( "The service is closed." );
	}
}