/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.Timer;

import distributed_fs.net.Networking.RUDPnet;
import distributed_fs.net.Networking.Session;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.TransferSpeed;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.manager.ThreadMonitor.ThreadState;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

/**
 * Class used to receive the quorum requests.
 * The request can be either a make or a release quorum.
*/
public class QuorumThread extends Thread
{
    private int port;
    private String address;
    private DFSNode node;
    private AtomicBoolean shutDown = new AtomicBoolean( false );
    
    private Timer timer;
    private long nextId = -1L;
    private long id = -1L;
    private final Map<String, QuorumFile> fileLock = new HashMap<>( 64 );
    private final ReentrantLock QUORUM_LOCK = new ReentrantLock( true );
    
    private static final int PORT_OFFSET = 4;
    private static final short BLOCKED_TIME = 10000; // 10 seconds.
    private static final byte MAKE_QUORUM = 0, RELEASE_QUORUM = 1;
    private static final byte ACCEPT_QUORUM_REQUEST = 0, DECLINE_QUORUM_REQUEST = 1;
    
    
    
    
    public QuorumThread( int port,
                         String address,
                         DFSNode node ) throws IOException
    {
        setName( "Quorum" );
        
        this.port = port + PORT_OFFSET;
        this.address = address;
        this.node = node;
        
        // Wake-up the timer every BLOCKED_TIME milliseconds,
        // and update the TimeToLive of each locked file.
        // If the TTL reachs 0, the file is removed from queue.
        timer = new Timer( BLOCKED_TIME, new ActionListener() {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                if(!shutDown.get()) {
                    QUORUM_LOCK.lock();
                    
                    Iterator<QuorumFile> it = fileLock.values().iterator();
                    while(it.hasNext()) {
                        QuorumFile qFile = it.next();
                        qFile.updateTTL( BLOCKED_TIME );
                        if(qFile.toDelete())
                            it.remove();
                    }
                    
                    QUORUM_LOCK.unlock();
                }
            }
        } );
        timer.start();
    }
    
    @Override
    public void run()
    {
        RUDPnet net;
        
        try{
            net = new RUDPnet( address, port );
            net.setSoTimeout( DFSNode.WAIT_CLOSE );
        }
        catch( IOException e ) {
            e.printStackTrace();
            return;
        }
        
        DFSNode.LOGGER.info( "QuorumThread launched." );
        
        byte[] msg;
        while(!shutDown.get()) {
            try {
                Session session = net.waitForConnection();
                if(session == null)
                    continue;
                
                // Read the request.
                ByteBuffer data = ByteBuffer.wrap( session.receive() );
                DFSNode.LOGGER.info( "[QUORUM] Received a connection from: " + session.getEndPointAddress() );
                
                switch( data.get() ) {
                    case( MAKE_QUORUM ):
                        byte opType = data.get();
                        String fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        boolean locked = lockFile( fileName, 0, opType );
                        
                        DFSNode.LOGGER.info( "[QUORUM] Received a MAKE_QUORUM request for '" + fileName +
                                             "'. Request status: " + (!locked ? "BLOCKED" : "FREE") );
                        
                        // Send the current blocked state.
                        if(!locked)
                            msg = new byte[]{ DECLINE_QUORUM_REQUEST };
                        else 
                            msg = net.createMessage( new byte[]{ ACCEPT_QUORUM_REQUEST }, DFSUtils.longToByteArray( id ), false );
                        session.sendMessage( msg, false );
                        
                        break;
                    
                    case( RELEASE_QUORUM ):
                        DFSNode.LOGGER.info( "[QUORUM] Received a RELEASE_QUORUM request" );
                        long id = data.getLong();
                        fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        unlockFile( fileName, id );
                        break;
                }
            }
            catch( IOException e ) {
                e.printStackTrace();
                break;
            }
        }
        
        net.close();
        
        DFSNode.LOGGER.info( "Quorum thread closed." );
    }
    
    private long getNextId()
    {
        return nextId = (nextId + 1) % Long.MAX_VALUE;
    }
    
    /**
     * Starts the quorum phase.
     * 
     * @param state         state of the caller thread
     * @param session       the actual connection
     * @param opType        the operation type
     * @param fileName      name of the file
     * @param destId        the destination virtual node
     * @param myAddress     the address of the node in the form {@code hostname:port}
     * 
     * @return list of contacted nodes that have agreed to the quorum
    */
    public List<QuorumNode> checkQuorum( ThreadState state,
                                         Session session,
                                         byte opType,
                                         String fileName,
                                         String destId ) throws IOException
    {
        // Get the list of successor nodes.
        List<GossipMember> nodes = state.getValue( ThreadState.SUCCESSOR_NODES );
        if(nodes == null) {
            String myAddress = address + ":" + (port - PORT_OFFSET);
            nodes = node.getSuccessorNodes( destId, myAddress, QuorumSession.getMaxNodes() );
            state.setValue( ThreadState.SUCCESSOR_NODES, nodes );
        }
        
        int size = nodes.size();
        DFSNode.LOGGER.debug( "[SN] Neighbours: " + size );
        if(size < QuorumSession.getMinQuorum( opType )) {
            // If there is a number of nodes less than the quorum,
            // we neither start the protocol.
            DFSNode.LOGGER.info( "[SN] Quorum failed, due to insufficient replica nodes: " +
                                 size + "/" + QuorumSession.getMinQuorum( opType ) );
            sendQuorumResponse( state, session, Message.TRANSACTION_FAILED );
            return new ArrayList<>();
        }
        else {
            List<QuorumNode> agreedNodes = contactNodes( state, session, opType, fileName, nodes );
            return agreedNodes;
        }
    }

    /**
     * Contacts the nodes to complete the quorum phase.
     * 
     * @param state          state of the caller thread
     * @param session        the actual connection
     * @param opType         the operation type
     * @param fileName       name of the file
     * @param nodes          list of contacted nodes
     * 
     * @return list of nodes that agreed to the quorum
    */
    private List<QuorumNode> contactNodes( ThreadState state,
                                           Session session,
                                           byte opType,
                                           String fileName,
                                           List<GossipMember> nodes ) throws IOException
    {
        List<QuorumNode> agreedNodes = state.getValue( ThreadState.AGREED_NODES );
        if(agreedNodes == null) {
            agreedNodes = new ArrayList<>( QuorumSession.getMaxNodes() );
            state.setValue( ThreadState.AGREED_NODES, agreedNodes );
        }
        
        RUDPnet net = new RUDPnet();
        
        Integer errors = state.getValue( ThreadState.QUORUM_ERRORS );
        if(errors == null) errors = 0;
        for(GossipMember node : nodes) {
            Session newSession = null;
            try {
                // Start the remote connection.
                if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                    DFSNode.LOGGER.info( "[SN] Contacting " + node + "..." );
                    newSession = net.tryConnect( node.getHost(), node.getPort() + PORT_OFFSET, 2000 );
                    state.setValue( ThreadState.AGREED_NODE_CONN, newSession );
                    state.getActionsList().addLast( DFSNode.DONE );
                }
                else {
                    newSession = state.getValue( ThreadState.AGREED_NODE_CONN );
                    state.getActionsList().removeFirst();
                }
                
                if(newSession == null)
                    throw new IOException();
                
                // Send the message.
                if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                    byte[] msg = net.createMessage( new byte[]{ MAKE_QUORUM, opType }, fileName.getBytes( StandardCharsets.UTF_8 ), true );
                    newSession.sendMessage( msg, true );
                    state.getActionsList().addLast( DFSNode.DONE );
                }
                else
                    state.getActionsList().remove();
                
                // Wait for the response message.
                DFSNode.LOGGER.info( "[SN] Waiting the response..." );
                ByteBuffer data;
                if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                    data = ByteBuffer.wrap( newSession.receive() );
                    state.setValue( ThreadState.QUORUM_MSG_RESPONSE, data );
                    state.getActionsList().addLast( DFSNode.DONE );
                }
                else {
                    data = state.getValue( ThreadState.QUORUM_MSG_RESPONSE );
                    state.getActionsList().remove();
                }
                
                newSession.close();
                
                // Read the response.
                if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                    if(data.get() == ACCEPT_QUORUM_REQUEST) {
                        // Not blocked => node agrees to the quorum.
                        DFSNode.LOGGER.info( "[SN] Node " + node + " agree to the quorum." );
                        QuorumNode qNode = new QuorumNode( node, fileName, opType, data.getLong() );
                        qNode.addAgreedNodes( agreedNodes );
                        agreedNodes.add( qNode );
                    }
                    else {
                        // Blocked => the node doesn't agree to the quorum.
                        DFSNode.LOGGER.info( "[SN] Node " + node + " doesn't agree to the quorum." );
                        if(QuorumSession.unmakeQuorum( ++errors, opType )) {
                            cancelQuorum( state, session, agreedNodes );
                            break;
                        }
                        state.setValue( ThreadState.QUORUM_ERRORS, errors );
                    }
                    
                    state.getActionsList().addLast( DFSNode.DONE );
                }
                else
                    state.getActionsList().removeFirst();
            }
            catch( IOException e ) {
                // Ignored.
                //e.printStackTrace();
                
                if(newSession != null)
                    newSession.close();
                
                // Unreachable node: check for the completion of the quorum.
                DFSNode.LOGGER.info( "[SN] Node " + node + " is not reachable." );
                if(QuorumSession.unmakeQuorum( ++errors, opType )) {
                    cancelQuorum( state, session, agreedNodes );
                    break;
                }
                state.setValue( ThreadState.QUORUM_ERRORS, errors );
            }
        }
        
        net.close();
        
        return agreedNodes;
    }
    
    /**
     * Closes the opened quorum requests.
     * 
     * @param state         state of the caller thread
     * @param session       network channel with the client
     * @param agreedNodes   list of contacted nodes
    */
    public void cancelQuorum( ThreadState state,
                              Session session,
                              List<QuorumNode> agreedNodes ) throws IOException
    {
        if(session != null)
            DFSNode.LOGGER.info( "[SN] The quorum cannot be reached. The transaction will be closed." );
        
        closeQuorum( state, agreedNodes );
        // send to the client the negative response
        sendQuorumResponse( state, session, Message.TRANSACTION_FAILED );
    }
    
    /**
     * Closes an open quorum session.
     * 
     * @param state         state of the caller thread
     * @param agreedNodes   list of agreed nodes
    */
    public void closeQuorum( ThreadState state, List<QuorumNode> agreedNodes )
    {
        TCPnet net = new TCPnet();
        for(int i = agreedNodes.size() - 1; i >= 0; i--) {
            GossipMember node = agreedNodes.get( i ).getNode();
            Session mySession = null;
            try {
                mySession = state.getValue( ThreadState.RELEASE_QUORUM_CONN );
                if(mySession == null || mySession.isClosed()) {
                    mySession = net.tryConnect( node.getHost(), node.getPort() + PORT_OFFSET, 2000 );
                    state.setValue( ThreadState.RELEASE_QUORUM_CONN, mySession );
                }
                
                // Create the message.
                byte[] msg = net.createMessage( new byte[]{ RELEASE_QUORUM }, DFSUtils.longToByteArray( agreedNodes.get( i ).getId() ), false );
                msg = net.createMessage( msg, agreedNodes.get( i ).getFileName().getBytes( StandardCharsets.UTF_8 ), true );
                
                if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                    mySession.sendMessage( msg, true );
                    state.getActionsList().addLast( DFSNode.DONE );
                }
                else
                    state.getActionsList().removeFirst();
                
                agreedNodes.remove( i );
            }
            catch( IOException e ) {
                // Ignored.
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
     * @param state     state of the caller thread
     * @param session   the actual TCP connection
     * @param response  the quorum response
    */
    public void sendQuorumResponse( ThreadState state,
                                    Session session,
                                    byte response ) throws IOException
    {
        if(session != null) {
            if(!state.isReplacedThread() || state.getActionsList().isEmpty()) {
                MessageResponse message = new MessageResponse( response );
                session.sendMessage( message, true );
                state.getActionsList().addLast( DFSNode.DONE );
            }
            else
                state.getActionsList().removeFirst();
        }
    }
    
    /**
     * Returns the file specified by its name.
     * 
     * @param fileName  name of the file to retrieve
     * 
     * @return the quorum file, if present, {@code null} otherwise
    */
    public QuorumFile getQuorumFile( String fileName )
    {
        QUORUM_LOCK.lock();
        QuorumFile qFile = fileLock.get( fileName );
        QUORUM_LOCK.unlock();
        
        return qFile;
    }
    
    /**
     * Tries to lock a file.
     * 
     * @param fileName  name of the file to lock
     * @param fileId    unique identifier associated to the operation
     * @param opType    type of operation ({@code GET}, {@code PUT} or {@code DELETE})
     * 
     * @return {@code true} if the file has been locked, {@code false} otherwise.
    */
    public boolean lockFile( String fileName, long fileId, byte opType )
    {
        return setLocked( true, fileName, fileId, opType );
    }
    
    /**
     * Tries to unlock a file.
     * 
     * @param fileName  name of the file to unlock
     * @param fileId    unique identifier associated to the operation
     * 
     * @return {@code true} if the file has been unlocked, {@code false} otherwise.
    */
    public boolean unlockFile( String fileName, long fileId )
    {
        return setLocked( false, fileName, fileId, (byte) 0x0 );
    }
    
    /**
     * Sets the locked state for a given file.
     * 
     * @param toLock    {@code true} to (try to) lock the file, {@code false} otherwise
     * @param fileName  name of the file
     * @param fileId    id of the quorum file
     * @param opType    operation type
     * 
     * @return {@code true} if the file has been locked, {@code false} otherwise.
    */
    private boolean setLocked( boolean toLock, String fileName, long fileId, byte opType )
    {
        boolean isLocked = true;
        
        QUORUM_LOCK.lock();
        
        QuorumFile qFile = fileLock.get( fileName );
        
        if(toLock) {
            if(qFile != null) {
                System.out.println( "RICHIESTO: " + fileName + ", TYPE: " + qFile.getOpType() );
                if(opType == Message.GET && qFile.getOpType() == opType) {
                    // Read locking.
                    qFile.setReaders( +1 );
                    id = qFile.getId();
                }
                else // The file is already locked in write mode.
                     isLocked = false;
            }
            else
                fileLock.put( fileName, new QuorumFile( id = getNextId(), opType ) );
        }
        else {
            // Lock released.
            if(qFile != null && fileId == qFile.getId()) {
                if(qFile.getOpType() == Message.GET)
                    qFile.setReaders( -1 );
                
                if(qFile.toDelete())
                    fileLock.remove( fileName );
            }
        }
        
        QUORUM_LOCK.unlock();
        
        return isLocked;
    }
    
    public void close()
    {
        shutDown.set( true );
        timer.stop();
        interrupt();
    }

    /**
     * Class used to manage the agreed nodes of the quorum.
    */
    public static class QuorumNode
    {
        private final GossipMember node;
        private List<QuorumNode> nodes;
        private final String fileName;
        private final byte opType;
        private final long id;
        
        public QuorumNode( GossipMember node, String fileName,
                           byte opType, long id )
        {
            this.node = node;
            this.fileName = fileName;
            this.opType = opType;
            this.id = id;
        }
        
        public GossipMember getNode() {
            return node;
        }
        
        /**
         * Method used, during the transmission of the files,
         * to set the list of agreed nodes.
        */
        public void addAgreedNodes( List<QuorumNode> nodes ) {
            this.nodes = nodes;
        }
        
        public byte getOpType() {
            return opType;
        }

        public List<QuorumNode> getList() {
            return nodes;
        }
        
        public long getId() {
            return id;
        }

        public String getFileName() {
            return fileName;
        }
    }
    
    /**
     * Class used to represent a file during the quorum phase.
     * This object remains in the Map as long as its TimeToLive
     * is less than a threshold.
    */
    public static class QuorumFile implements TransferSpeed
    {
        /** Maximum waiting time of the file in the Map. */
        private static final long DEFAULT_TTL = 60000; // 1 Minute.
        
        private long id;
        private long ttl = 0, maxTTL;
        private byte opType;
        private int readers = 0;
        private boolean isUpdated = false;
        
        public QuorumFile( long id, byte opType )
        {
            this.id = id;
            this.opType = opType;
            if(opType == Message.GET)
                readers = 1;
            
            maxTTL = DEFAULT_TTL;
        }
        
        /**
         * Updates the TimeToLive of the file.
         * 
         * @param delta   amount of time to add
        */
        public void updateTTL( int delta )
        {
            if(!isUpdated())
                ttl += delta;
        }
        
        public boolean toDelete()
        {
            return readers == 0 || ttl >= maxTTL;
        }
        
        /**
         * Changes the number of readers.
         * 
         * @param value   +1/-1
        */
        public void setReaders( int value )
        {
            readers += value;
            if(value == +1) // Restart the time to live.
                ttl = 0;
        }
        
        public byte getOpType()
        {
            return opType;
        }
        
        public long getId()
        {
            return id;
        }
        
        private synchronized boolean isUpdated() {
            boolean updated = isUpdated;
            updated = false;
            return updated;
        }
        
        private synchronized void setUpdated() {
            ttl = 0;
            isUpdated = true;
        }

        @Override
        public void update( int bytesToReceive, double throughput )
        {
            setUpdated();
            final int DELAY = 10;
            // Compute the time to download the file + a little constant,
            // just to be sure of the complete download of the file itself.
            int time = ((int) (bytesToReceive / throughput) + DELAY) * 1000;
            maxTTL = time;
        }
    }
    
    public static class QuorumSession
    {
        // Parameters of the quorum protocol.
        private static final short N = 3; // Total number of nodes.
        private static final short W = 2; // Number of writers.
        private static final short R = 2; // Number of readers.
        
        /**
         * Gets the maximum number of nodes to contact
         * for the quorum protocol.
        */
        public static short getMaxNodes() {
            return N - 1;
        }

        public static short getWriters() {
            return W - 1;
        }
        
        public static short getReaders() {
            return R - 1;
        }
        
        public static boolean isReadQuorum( int readers ) {
            return readers >= getReaders();
        }
        
        public static boolean isWriteQuorum( int writers ) {
            return writers >= getWriters();
        }
        
        public static boolean isDeleteQuorum( int deleters ) {
            return deleters >= getWriters();
        }
        
        public static boolean isQuorum( byte opType, int replicaNodes )
        {
            if(opType == Message.PUT || opType == Message.DELETE)
                return isWriteQuorum( replicaNodes );
            else
                return isReadQuorum( replicaNodes );
        }
        
        public static boolean unmakeQuorum( int errors, byte opType )
        {
            if(opType == Message.PUT || opType == Message.DELETE)
                return (getMaxNodes() - errors) < getWriters();
            else
                return (getMaxNodes() - errors) < getReaders();
        }
        
        public static int getMinQuorum( byte opType )
        {
            if(opType == Message.GET)
                return getReaders();
            else
                return getWriters();
        }
    }
}
