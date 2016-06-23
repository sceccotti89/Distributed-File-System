/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.json.JSONException;

import com.google.common.base.Preconditions;

import distributed_fs.client.manager.ClientSynchronizer;
import distributed_fs.client.manager.DFSManager;
import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.RemoteFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;
import gossiping.GossipMember;

public class DFSService extends DFSManager implements IDFSService
{
	private final Random random;
	private final DFSDatabase database;
	private ClientSynchronizer syncClient;
	private String hintedHandoff = null;
	
	private boolean disableSyncThread = false;
	
	private boolean testing = false;
	private boolean initialized = false;
	
	public DFSService( final String ipAddress,
					   final int port,
					   final boolean useLoadBalancer,
					   final List<GossipMember> members,
					   final String resourcesLocation,
					   final String databaseLocation,
					   final DBListener listener ) throws IOException, JSONException, DFSException
	{
		super( ipAddress, port, useLoadBalancer, members );
		
		random = new Random();
		database = new DFSDatabase( resourcesLocation, databaseLocation, null );
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
	*/
	public void disableSyncThread() {
		disableSyncThread = true;
	}
	
	/**
	 * Starts the service.
	 * 
	 * @return {@code true} if the service has been started,
	 * 		   {@code false} otherwise.
	*/
	public boolean start() throws IOException
	{
		if(!disableSyncThread) {
		    syncClient = new ClientSynchronizer( this, database );
		    syncClient.start();
		}
	    
	    if(!useLoadBalancer)
            listMgr_t.start();
		
		LOGGER.info( "System up." );
		return (initialized = true);
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	 * @throws InterruptedException 
	*/
	public DistributedFile getFile( final String fileName ) throws InterruptedException
	{
		DistributedFile file = database.getFile( fileName );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Retrieves all the files stored in a random node of the network.
	 * 
	 * @return list of files, if everything was ok, {@code null} otherwise.
	*/
	public List<RemoteFile> getAllFiles() throws DFSException
	{
		LOGGER.info( "Synchonizing..." );
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		List<RemoteFile> files = null;
		boolean completed = true;
		
		if(!contactRemoteNode( "", Message.GET_ALL ))
		    return null;
		
		try{
			//LOGGER.debug( "Sending message..." );
			sendGetAllMessage( "" );
			//LOGGER.debug( "Request message sent" );
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if(!checkResponse( session, "GET_ALL", true ))
	                throw new IOException();
			    
			    if(!waitRemoteConnection())
                    throw new IOException();
            }
			
			files = readGetAllResponse( session );
		}
		catch( IOException e ) {
		    e.printStackTrace();
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
		Preconditions.checkNotNull( fileName );
		
		if(!initialized) {
		    throw new DFSException( "The system has not been initialized.\n" +
		                            "Use the \"start\" method to initialize the system." );
		}
		
		LOGGER.info( "Starting GET operation: " + fileName );
		RemoteFile toWrite;
		DistributedFile backToClient;
		
		String normFileName = database.normalizeFileName( fileName );
		if(!contactRemoteNode( normFileName, Message.GET ))
			return null;
		
		try {
			// Send the request.
			//LOGGER.info( "Sending message..." );
			sendGetMessage( normFileName );
			//LOGGER.info( "Message sent" );
			
			// Checks whether the load balancer founded an available node,
			// or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "GET", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if(!waitRemoteConnection() ||
    			   !checkResponse( session, "GET", true ))
    				throw new IOException();
			}
			
			// Receive one or more files.
			List<RemoteFile> files = readGetResponse( session );
			
			if(files.size() == 0) {
				LOGGER.info( "File \"" + fileName + "\" not found." );
				session.close();
				return null;
			}
			
			//LOGGER.info( "Received " + files.size() + " files." );
			
			int id = 0;
			if(files.size() > 1) {
				List<VectorClock> versions = new ArrayList<>();
				for(RemoteFile file : files)
					versions.add( file.getVersion() );
				
				id = syncClient.makeReconciliation( fileName, versions );
				// Send back the reconciled version.
				if(!put( files.get( id ).getName() )) {
					throw new IOException();
				}
			}
			
			// Update the database.
			toWrite = files.get( id );
			if(database.saveFile( toWrite, toWrite.getVersion(), null, true ) != null)
				backToClient = new DistributedFile( toWrite, toWrite.isDirectory(), null );
			else
				backToClient = getFile( fileName );
		}
		catch( IOException | SQLException | InterruptedException e ) {
			LOGGER.info( "Operation GET not performed. Try again later." );
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
		Preconditions.checkNotNull( fileName );
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		LOGGER.info( "Starting PUT operation: " + fileName );
		
		String normFileName = database.normalizeFileName( fileName );
		
		DistributedFile file = database.getFile( fileName );
		File f = new File( database.getFileSystemRoot() + normFileName );
		//System.out.println( "FILE: " + file );
		if(file == null || !DFSUtils.existFile( f, false )){
		    if(file == null && database.checkExistFile( fileName ))
				file = new DistributedFile( normFileName, f.isDirectory(), new VectorClock(), null );
			else {
				LOGGER.error( "Operation PUT not performed: file \"" + fileName + "\" not founded. " );
				LOGGER.error( "The file must be present in one of the sub-directories of the root: " + database.getFileSystemRoot() );
				return false;
			}
		}
		
		file.setDeleted( false );
		System.out.println( "FILE VERSION: " + file.getVersion() );
		
        if(!contactRemoteNode( normFileName, Message.PUT ))
			return false;
		
		boolean completed = true;
		
		try {
			//LOGGER.info( "Sending file..." );
			
			// Send the file.
			RemoteFile rFile = new RemoteFile( file, database.getFileSystemRoot() );
			sendPutMessage( rFile, hintedHandoff );
			//LOGGER.info( "File sent" );
			
			// Checks whether the load balancer founded an available node,
            // or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "PUT", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if(!waitRemoteConnection() ||
    			   !checkResponse( session, "PUT", true ))
    				throw new IOException();
			}
			
			// Update the file's vector clock.
			MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
            if(message.getType() == (byte) 0x1) {
                LOGGER.debug( "Updating the clock of the file '" + fileName + "'..." );
                VectorClock newClock = DFSUtils.deserializeObject( message.getObjects().get( 0 ) );
    			database.saveFile( rFile, newClock, null, true );
            }
		}
		catch( IOException | SQLException e ) {
			//e.printStackTrace();
			LOGGER.info( "Operation PUT '" + fileName + "' not performed. Try again later." );
			completed = false;
		}
		
		if(completed)
			LOGGER.info( "Operation PUT '" + fileName + "' successfully completed." );
		
		session.close();
		
		return completed;
	}
	
	@Override
	public boolean delete( final String fileName ) throws IOException, DFSException
	{
		Preconditions.checkNotNull( fileName );
		
		if(!initialized) {
            throw new DFSException( "The system has not been initialized.\n" +
                                    "Use the \"start\" method to initialize the system." );
        }
		
		boolean completed = true;
		String normFileName = database.normalizeFileName( fileName );
		
		DistributedFile file = database.getFile( fileName );
		if(file == null || !DFSUtils.existFile( database.getFileSystemRoot() + normFileName, false )) {
			LOGGER.error( "File \"" + fileName + "\" not founded." );
			return false;
		}
		
		if(file.isDirectory()) {
		    // Delete recursively all the files present in the directory.
		    File inputFile = new File( database.getFileSystemRoot() + normFileName );
			for(File f: inputFile.listFiles()) {
				LOGGER.debug( "Name: " + f.getPath() + ", Directory: " + f.isDirectory() );
				completed |= delete( f.getPath() );
				if(!completed)
					break;
			}
		}
		
		LOGGER.info( "starting DELETE operation for: " + fileName );
		
		if(!contactRemoteNode( normFileName, Message.DELETE ))
			return false;
		
		try {
			sendDeleteMessage( file, hintedHandoff );
			
			// Checks whether the load balancer founded an available node,
            // or (with no balancers) if the quorum has been completed successfully.
			if(!checkResponse( session, "DELETE", false ))
				throw new IOException();
			
			if(useLoadBalancer) {
			    // Checks whether the request has been forwarded to the storage node.
			    if(!waitRemoteConnection() ||
    			   !checkResponse( session, "DELETE", true ))
    				throw new IOException();
			}
			
			// Update the file's vector clock.
			MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
			if(message.getType() == (byte) 0x1) {
			    LOGGER.debug( "Updating the clock of the file '" + fileName + "'..." );
    			VectorClock newClock = DFSUtils.deserializeObject( message.getObjects().get( 0 ) );
    			database.removeFile( fileName, newClock, null );
			}
		}
		catch( IOException | SQLException e ) {
			//e.printStackTrace();
			LOGGER.info( "Operation DELETE not performed. Try again later." );
			completed = false;
		}
		
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
     * Try a connection with the first available remote node.
     * 
     * @param fileName    name of the file
     * @param opType      operation type
     * 
     * @return {@code true} if at least one remote node is available,
     *         {@code false} otherwise.
    */
	private boolean contactRemoteNode( final String fileName, final byte opType )
	{
	    if(useLoadBalancer && contactLoadBalancerNode())
	        return true;
	    
	    if(!useLoadBalancer && contactStorageNode( fileName, opType ))
	        return true;
	    
	    return false;
	}
	
	/**
	 * Try a connection with the first available LoadBalancer node.
	 * 
	 * @return {@code true} if at least one remote node is available,
	 * 		   {@code false} otherwise.
	*/
	private boolean contactLoadBalancerNode()
	{
		LOGGER.info( "Contacting a balancer node..." );
		
		session = null;
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
				LOGGER.info( "Contacting " + partner + "..." );
				try{ session = net.tryConnect( partner.getHost(), partner.getPort(), 2000 ); }
				catch( IOException e ) {
					//e.printStackTrace();
					nodes.remove( partner );
					System.out.println( "Node " + partner + " unreachable." );
				}
			}
		}
		
		if(session == null) {
			LOGGER.error( "Sorry, but the service is not available. Retry later." );
			return false;
		}
		
		return true;
	}
	
	/**
     * Try a connection with the first available StorageNode node.
     * 
     * @param fileName    name of the file
     * @param opType      operation type
     * 
     * @return {@code true} if at least one remote node is available,
     *         {@code false} otherwise.
    */
    private boolean contactStorageNode( final String fileName, final byte opType )
    {
        LOGGER.info( "Contacting a storage node..." );
        
        String fileId = DFSUtils.getId( fileName );
        session = null;
        List<GossipMember> nodes = null;
        synchronized( cHasher ) {
            String nodeId = cHasher.getNextBucket( fileId );
            GossipMember node = cHasher.getBucket( nodeId );
            nodes = getNodesFromPreferenceList( nodeId, node );
        }
        
        boolean nodeDown = false;
        if(nodes != null) {
            hintedHandoff = null;
            for(GossipMember member : nodes) {
                LOGGER.debug( "[CLIENT] Contacting: " + member );
                try{
                    session = net.tryConnect( member.getHost(), member.getPort(), 2000 );
                    destId = member.getId();
                    return true;
                }
                catch( IOException e ) {
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
        
        // Start the background thread
        // for the membership.
        if(nodeDown)
            listMgr_t.wakeUp();
        
        LOGGER.error( "Sorry, but the service is not available. Retry later." );
        return false;
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
        List<GossipMember> nodes = getSuccessorNodes( id, sourceNode.getHost(), PREFERENCE_LIST );
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
        
        if(!DFSUtils.testing)
            filterAddress.add( addressToRemove );
        
        // Choose the nodes whose address is different than this node
        String currId = id, succ;
        while(size < numNodes) {
            succ = cHasher.getNextBucket( currId );
            if(succ == null || succ.equals( id ))
                break;
            
            GossipMember node = cHasher.getBucket( succ );
            if(node != null) {
                currId = succ;
                if(!filterAddress.contains( node.getHost() )) {
                    nodes.add( node );
                    if(!DFSUtils.testing)
                        filterAddress.add( node.getHost() );
                    size++;
                }
            }
        }
        
        return nodes;
    }

    /**
	 * Wait the incoming connection from the StorageNode.
	 * 
	 * @return {@code true} if the connection if it has been established,
	 * 		   {@code false} otherwise
	*/
	private boolean waitRemoteConnection() throws IOException
	{
	    session.close();
		LOGGER.info( "Wait the incoming connection..." );
		session = net.waitForConnection();
		return (session != null);
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
		return closed;
	}
	
	@Override
	public void shutDown()
	{
	    super.shutDown();
		
		if(session != null && !session.isClosed()) {
			//try{ session.sendMessage( new MessageRequest( Message.CLOSE ), true ); }
			//catch( IOException e ) {}
			session.close();
		}
		net.close();
		
		if(!testing)
			database.close();
		
		LOGGER.info( "The service is closed." );
	}

    public static interface DBListener
    {
    	public void dbEvent( final String fileName, final byte code );
    }
}