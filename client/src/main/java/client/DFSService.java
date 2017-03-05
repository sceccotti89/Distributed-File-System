/**
 * @author Stefano Ceccotti
*/

package client;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;

import client.manager.ClientSynchronizer;
import client.manager.DFSManager;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.Session;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.manager.QuorumThread.QuorumSession;
import distributed_fs.storage.DBManager.DBListener;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.VersioningUtils;
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
    private boolean initialized = false;
    
    
    
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
     * Enables or disables the synchronized thread.
     * 
     * @param enable   {@code true} if the syncronized thread must be enabled,
     *                 {@code false} otherwise
     * 
     * @return this builder
    */
    public DFSService setSyncThread( final boolean enable )
    {
        disableSyncThread = !enable;
        if(enable) {
            syncClient = new ClientSynchronizer( this, database );
            syncClient.start();
        }
        else {
            if(syncClient != null)
                syncClient.shutDown( true );
        }
        
        return this;
    }
    
    /**
     * Starts the service.
     * 
     * @return {@code true} if the service has been successfully started,
     *            {@code false} otherwise.
    */
    public boolean start() throws IOException
    {
        database.newInstance();
        
        if(!disableSyncThread) {
            syncClient = new ClientSynchronizer( this, database );
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
        
        double startTime = System.currentTimeMillis();
        // Here the name of the file is not important.
        final String fileName = "";
        Session session = null;
        if((session = contactRemoteNode( fileName, Message.GET_ALL )) == null)
            return null;
        
        try{
            sendGetAllMessage( session, fileName );
            
            if(useLoadBalancer) {
                // Checks whether the request has been forwarded to the storage node.
                if(!checkResponse( session, true ))
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
        if(completed) {
            double completeTime = ((double) System.currentTimeMillis() - startTime) / 1000d;
            LOGGER.info( "Operation GET_ALL successfully completed in " + completeTime + " seconds." );
        }
        
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
        
        double startTime = System.currentTimeMillis();
        
        String normFileName = database.normalizeFileName( fileName );
        DistributedFile backToClient = database.getFile( normFileName );
        
        Session session = null;
        if((session = contactRemoteNode( normFileName, Message.GET )) == null)
            return null;
        
        try {
            // Send the request.
            sendGetMessage( session, normFileName, backToClient );
            
            // Checks whether the load balancer has found an available node,
            // or (with no balancers) if the quorum has been completed successfully.
            if(!checkResponse( session, false ))
                throw new IOException();
            
            if(useLoadBalancer) {
                session = waitRemoteConnection( session );
                // Checks whether the request has been forwarded to the storage node.
                if(!checkResponse( session, true ))
                    throw new IOException();
            }
            
            // Receive the response.
            byte[] result = session.receive();
            if(result.equals( Message.NOT_FOUND ) || result.equals( Message.UPDATED )) {
                session.close();
                if(result.equals( Message.NOT_FOUND )) {
                    LOGGER.info( "File \"" + fileName + "\" not found." );
                    return null;
                }
                else { // Message.UPDATED.
                    LOGGER.info( "File \"" + fileName + "\" is already updated." );
                    return backToClient;
                }
            }
            
            // Receive the retrieved files.
            List<DistributedFile> files = readGetResponse( session );
            int size = files.size();
            
            int id = 0;
            VectorClock clock = new VectorClock();
            boolean reconciled = false, updated = false;
            
            backToClient = database.getFile( fileName );
            if(backToClient == null) {
                // Choose the version among the only received files.
                reconciled = (size > 1);
                updated = true;
                id = ClientSynchronizer.makeReconciliation( files );
            }
            else {
                files.add( backToClient );
                files = VersioningUtils.makeReconciliation( files );
                if(files.size() > 1) {
                    // Ask the user which is the correct version.
                    id = ClientSynchronizer.makeReconciliation( files );
                    if(id < size) updated = true;
                    reconciled = true;
                    size++;
                }
            }
            
            if(updated) {
                // Generate only one clock, merging all the received versions.
                for(DistributedFile file : files)
                    clock = clock.merge( file.getVersion() );
                
                // Update the database.
                backToClient = files.get( id );
                if(backToClient.isDeleted())
                    database.deleteFile( fileName, clock, backToClient.isDirectory(), null );
                else
                    database.saveFile( backToClient, clock, null, true );
            }
            
            if(backToClient == null)
                backToClient = database.getFile( fileName );
            
            // Send back the reconciled version.
            if(size > 1 && reconciled) {
                // If the operation is not performed an exception is thrown.
                if((!backToClient.isDeleted() && !put( fileName )) ||
                    (backToClient.isDeleted() && !delete( fileName )))
                    throw new IOException();
            }
        }
        catch( IOException e ) {
            LOGGER.info( "Operation GET not performed. Try again later." );
            //e.printStackTrace();
            session.close();
            return null;
        }
        
        session.close();
        double completeTime = ((double) System.currentTimeMillis() - startTime) / 1000d;
        LOGGER.info( "Operation GET successfully completed in " + completeTime + " seconds. Received: " + backToClient );
        
        return backToClient;
    }
    
    @Override
    public boolean put( final String fileName ) throws DFSException, IOException
    {
        return doWrite( Message.PUT, fileName );
    }
    
    @Override
    public boolean delete( final String fileName ) throws IOException, DFSException
    {
        return doWrite( Message.DELETE, fileName );
    }
    
    /**
     * Computes the write operation on the given file name.
     * 
     * @param opType    the type ({@code PUT} or {@code DELETE}) of the operation
     * @param fileName  name of the file
     * 
     * @return {@code true} if the operation has been completed successfully,
     *         {@code false} otherwise
    */
    private boolean doWrite( final byte opType, final String fileName ) throws IOException, DFSException
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
        
        String opCode = getOpCode( opType );
        LOGGER.info( "Starting " + opCode + " operation for: " + fileName );
        
        boolean completed = true;
        
        String dbRoot = database.getFileSystemRoot();
        String normFileName = database.normalizeFileName( fileName );
        if(!database.existFile( dbRoot + normFileName, false )) {
            LOGGER.error( "Operation " + opCode + " not performed: file \"" + fileName + "\" not found. " );
            LOGGER.error( "The file must be present in one of the sub-directories starting from: " + dbRoot );
            return false;
        }
        
        DistributedFile file = database.getFile( fileName );
        if(file == null) {
            File f = new File( dbRoot + normFileName );
            file = new DistributedFile( normFileName, f.isDirectory(), new VectorClock(), null );
        }
        
        double startTime = System.currentTimeMillis();
        Session session = null;
        if((session = contactRemoteNode( normFileName, opType )) == null)
            return false;
        
        try {
            if(opType == Message.DELETE)
                sendDeleteMessage( session, file, hintedHandoff );
            else{
                // PUT operation.
                file.setDeleted( false );
                file.loadContent( database );
                sendPutMessage( session, file, hintedHandoff );
            }
            
            // Checks whether the load balancer has found an available node,
            // or (with no balancers) if the quorum has been completed successfully.
            if(!checkResponse( session, false ))
                throw new IOException();
            
            if(useLoadBalancer) {
                session = waitRemoteConnection( session );
                // Checks whether the request has been forwarded to the storage node.
                if(!checkResponse( session, true ))
                    throw new IOException();
            }
            
            // Update the file's vector clock.
            MessageResponse message = session.receiveMessage();
            LOGGER.debug( "Updating file: " + (message.getType() == (byte) 0x1) );
            if(message.getType() == (byte) 0x1) {
                LOGGER.debug( "Updating version of the file '" + fileName + "'..." );
                String nodeId = new String( message.getObjects().get( 0 ), StandardCharsets.UTF_8 );
                VectorClock newClock = file.getVersion().incremented( nodeId );
                if(opType == Message.PUT)
                    database.saveFile( file, newClock, null, false );
                else
                    database.deleteFile( fileName, newClock, file.isDirectory(), null );
            }
        }
        catch( IOException e ) {
            e.printStackTrace();
            LOGGER.info( "Operation " + opCode + " for \"" + fileName + "\" not performed. Try again later." );
            completed = false;
        }
        
        if(completed) {
            double completeTime = ((double) System.currentTimeMillis() - startTime) / 1000d;
            LOGGER.info( "Operation " + opCode + " '" + fileName + "' successfully completed in " + completeTime + " seconds." );
        }
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
    private Session contactRemoteNode( final String fileName, final byte opType )
    {
        Session session;
        
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
     *            {@code false} otherwise.
    */
    private Session contactLoadBalancerNode( final byte opType )
    {
        if(opType != Message.GET_ALL)
            LOGGER.info( "Contacting a balancer node..." );
        
        Session session = null;
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
    private Session contactStorageNode( final String fileName, final byte opType )
    {
        if(opType != Message.GET_ALL)
            LOGGER.info( "Contacting a storage node..." );
        
        String fileId = DFSUtils.getId( fileName );
        Session session = null;
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
     *            {@code null} otherwise
    */
    private Session waitRemoteConnection( final Session session ) throws IOException
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
     *            {@code false} otherwise.
    */
    public boolean isClosed()
    {
        return closed.get();
    }
    
    /**
     * Checks whether the system is trying
     * to reconcile concurrent versions.
    */
    public boolean isReconciling() {
        return syncClient != null && syncClient.getReconciliation();
    }
    
    @Override
    public void shutDown()
    {
        super.shutDown();
        
        if(!disableSyncThread)
            syncClient.shutDown( true );
        
        net.close();
        database.close();
        
        LOGGER.info( "The service is closed." );
    }
}
