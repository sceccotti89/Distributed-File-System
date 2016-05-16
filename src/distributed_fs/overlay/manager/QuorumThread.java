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

import javax.swing.Timer;

import org.json.JSONException;

import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.DFSnode;
import distributed_fs.overlay.StorageNode;
import distributed_fs.overlay.StorageNode.QuorumFile;
import distributed_fs.overlay.StorageNode.QuorumNode;
import distributed_fs.utils.QuorumSystem;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;

/**
 * Class used to receive the quorum requests.
 * The request can be a make or a release quorum.
*/
public class QuorumThread extends Thread
{
    private int port;
    private String address;
    private Timer timer;
    private Long id = (long) -1;
    private StorageNode node;
    private boolean shutDown;
    private final Map<String, QuorumFile> fileLock = new HashMap<>( 64 );
    
    private static final int BLOCKED_TIME = 5000; // 5 seconds.
    private static final byte MAKE_QUORUM = 0, RELEASE_QUORUM = 1;
    private static final byte ACCEPT_QUORUM_REQUEST = 0, DECLINE_QUORUM_REQUEST = 1;
    //private static final int QUORUM_PORT = 2500;
    
    public QuorumThread( final int port,
                         final String address,
                         final StorageNode node ) throws IOException, JSONException
    {
        this.port = port + 3;
        this.address = address;
        this.node = node;
        
        // Wake-up the timer every BLOCKED_TIME milliseconds,
        // and update the TimeToLive of each locked file.
        // If the TTL reachs 0, the file is removed from queue.
        timer = new Timer( BLOCKED_TIME, new ActionListener() {
            @Override
            public void actionPerformed( final ActionEvent e )
            {
                if(!shutDown) {
                    synchronized ( fileLock ) {
                        Iterator<QuorumFile> it = fileLock.values().iterator();
                        while(it.hasNext()) {
                            QuorumFile qFile = it.next();
                            qFile.updateTTL( BLOCKED_TIME );
                            if(qFile.toDelete())
                                it.remove();
                        }
                    }
                }
            }
        } );
        timer.start();
        
        // TODO sta parte non va bene
        List<QuorumNode> nodes = QuorumSystem.loadState();
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
            net = new TCPnet( address, port );
            net.setSoTimeout( DFSnode.WAIT_CLOSE );
        }
        catch( IOException e ) {
            e.printStackTrace();
            return;
        }
        
        byte[] msg;
        while(!shutDown) {
            try {
                //LOGGER.info( "[QUORUM] Waiting on " + _address + ":" + port );
                TCPSession session = net.waitForConnection();
                if(session == null)
                    continue;
                
                // read the request
                //byte[] data = net.receiveMessage();
                //LOGGER.info( "[QUORUM] Received a connection from: " + net.getSrcAddress() );
                
                ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
                DFSnode.LOGGER.info( "[QUORUM] Received a connection from: " + session.getSrcAddress() );
                
                switch( data.get() ) {
                    case( MAKE_QUORUM ):
                        byte opType = data.get();
                        String fileName = new String( Utils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        DFSnode.LOGGER.info( "Received a MAKE_QUORUM request for '" + fileName +
                                             "'. Actual status: " + (node.getBlocked( fileName, opType ) ? "BLOCKED" : "FREE") );
                        
                        // Send the current blocked state.
                        boolean blocked = node.getBlocked( fileName, opType );
                        //boolean blocked = getBlocked();
                        if(blocked)
                            msg = new byte[]{ DECLINE_QUORUM_REQUEST };
                        else {
                            /*if(opType == Message.GET)
                                msg = new byte[]{ ACCEPT_QUORUM_REQUEST };
                            else*/
                            msg = net.createMessage( new byte[]{ ACCEPT_QUORUM_REQUEST }, getNextId( opType ), false );
                        }
                        session.sendMessage( msg, false );
                        
                        //net.sendMessage( data, InetAddress.getByName( net.getSrcAddress() ), net.getSrcPort() );
                        //LOGGER.info( "[QUORUM] Type: " + getCodeString( opType ) );
                        //if(opType != Message.GET && !blocked)
                            //setBlocked( true, id );
                        node.setBlocked( true, fileName, id, opType );
                        
                        break;
                    
                    case( RELEASE_QUORUM ):
                        DFSnode.LOGGER.info( "Received a RELEASE_QUORUM request" );
                        long id = data.getLong();
                        fileName = new String( Utils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        node.setBlocked( false, fileName, id, (byte) 0x0 ); // Here the operation type is useless.
                        break;
                }
            }
            catch( IOException e ) {
                //e.printStackTrace();
                break;
            }
        }
        
        net.close();
        
        DFSnode.LOGGER.info( "Quorum thread closed." );
    }
    
    private byte[] getNextId( final byte opType )
    {
        synchronized( id ) {
            id = (id + 1) % Long.MAX_VALUE;
            return Utils.longToByteArray( id );
        }
    }
    
    @Override
    public long getId()
    {
        synchronized( id ) {
            return id;
        }
    }
    
    public void close()
    {
        shutDown = true;
        timer.stop();
        interrupt();
        fileLock.clear();
    }
    
    /**
     * Starts the quorum phase.
     * 
     * @param session   the actual TCP connection
     * @param opType    
     * @param fileName  
     * @param destId    
     * 
     * @return list of contacted nodes, that have agreed to the quorum
    */
    public List<QuorumNode> checkQuorum( final TCPSession session,
                                         final byte opType,
                                         final String fileName,
                                         final String destId ) throws IOException
    {
        ByteBuffer id = Utils.hexToBytes( destId );
        
        List<GossipMember> nodes = node.getSuccessorNodes( id, address, QuorumSystem.getMaxNodes() );
        
        DFSnode.LOGGER.debug( "Neighbours: " + nodes.size() );
        if(nodes.size() < QuorumSystem.getMinQuorum( opType )) {
            DFSnode.LOGGER.info( "[SN] Quorum failed: " + nodes.size() + "/" + QuorumSystem.getMinQuorum( opType ) );
            
            // If there is a number of nodes less than the quorum,
            // we neither start the protocol.
            sendQuorumResponse( session, Message.TRANSACTION_FAILED );
            return new ArrayList<>();
        }
        else {
            List<QuorumNode> nodeAddress = contactNodes( session, opType, fileName, nodes );
            return nodeAddress;
        }
    }

    /**
     * Contacts the nodes to complete the quorum phase.
     * 
     * @param session   
     * @param opType    
     * @param fileName  
     * @param nodes     
     * 
     * @return list of contacted nodes, that have agreed to the quorum
    */
    private List<QuorumNode> contactNodes( final TCPSession session,
                                           final byte opType,
                                           final String fileName,
                                           final List<GossipMember> nodes ) throws IOException
    {
        int errors = 0;
        List<QuorumNode> agreedNodes = new ArrayList<>();
        
        //UDPnet net = new UDPnet();
        TCPnet net = new TCPnet();
        //net.setSoTimeout( 2000 );
        
        for(GossipMember node : nodes) {
            DFSnode.LOGGER.info( "[SN] Contacting " + node + "..." );
            TCPSession mySession = null;
            try {
                mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
                byte[] msg = net.createMessage( new byte[]{ MAKE_QUORUM, opType }, fileName.getBytes( StandardCharsets.UTF_8 ), true );
                mySession.sendMessage( msg, true );
                //mySession.sendMessage( new byte[]{ MAKE_QUORUM, opType }, false );
                
                //net.sendMessage( new byte[]{ MAKE_QUORUM }, InetAddress.getByName( node.getHost() ), QUORUM_PORT );
                //net.sendMessage( new byte[]{ MAKE_QUORUM }, InetAddress.getByName( node.getHost() ), node.getPort() + 3 );
                DFSnode.LOGGER.info( "[SN] Waiting the response..." );
                //byte[] data = net.receiveMessage();
                ByteBuffer data = ByteBuffer.wrap( mySession.receiveMessage() );
                mySession.close();
                
                if(data.get() == ACCEPT_QUORUM_REQUEST) {
                    DFSnode.LOGGER.info( "[SN] Node " + node + " agree to the quorum." );
                    // Not blocked => agree to the quorum.
                    QuorumNode qNode = new QuorumNode( node, fileName, opType, data.getLong() );
                    qNode.addList( agreedNodes );
                    agreedNodes.add( qNode );
                    QuorumSystem.saveState( agreedNodes );
                }
                else {
                    // Blocked => the node doesn't agree to the quorum.
                    DFSnode.LOGGER.info( "[SN] Node " + node + " doesn't agree to the quorum." );
                    if(QuorumSystem.unmakeQuorum( ++errors, opType )) {
                        cancelQuorum( session, agreedNodes );
                        break;
                    }
                }
            }
            catch( IOException | JSONException e ) {
                if(mySession != null)
                    mySession.close();
                
                DFSnode.LOGGER.info( "[SN] Node " + node + " is not reachable." );
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
     * @param session       network channel with the client
     * @param agreedNodes   list of contacted nodes
    */
    public void cancelQuorum( final TCPSession session, final List<QuorumNode> agreedNodes ) throws IOException
    {
        if(session != null)
            DFSnode.LOGGER.info( "[SN] The quorum cannot be reached. The transaction will be closed." );
        
        closeQuorum( agreedNodes );
        // send to the client the negative response
        sendQuorumResponse( session, Message.TRANSACTION_FAILED );
    }
    
    public void closeQuorum( final List<QuorumNode> agreedNodes )
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
                //mySession.sendMessage( new byte[]{ RELEASE_QUORUM }, false );
                byte[] msg = net.createMessage( new byte[]{ RELEASE_QUORUM },
                                                Utils.longToByteArray( agreedNodes.get( i ).getId() ),
                                                false );
                msg = net.createMessage( msg,
                                         agreedNodes.get( i ).getFileName().getBytes( StandardCharsets.UTF_8 ),
                                         true );
                mySession.sendMessage( msg, true );
                
                agreedNodes.remove( i );
                QuorumSystem.saveState( agreedNodes );
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
    public void sendQuorumResponse( final TCPSession session, final byte response ) throws IOException
    {
        if(session != null) {
            MessageResponse message = new MessageResponse( response );
            session.sendMessage( message, true );
        }
    }
    
    /**
     * Sets the locked state for a given file.
     * 
     * @param blocked   {@code true} for locking state, {@code false} otherwise
     * @param fileName  name of the file
     * @param id        id of the quorum
     * @param opType    operation type
    */
    public void setBlocked( final boolean blocked, final String fileName, final long id, final byte opType )
    {
        synchronized ( fileLock ) {
            QuorumFile qFile = fileLock.get( fileName );
            
            if(blocked) {
                if(qFile != null)
                    qFile.setReaders( +1 );
                else
                    fileLock.put( fileName, new QuorumFile( id, opType ) );
            }
            else {
                if(qFile != null && id == qFile.getId()) {
                    if(qFile.getOpType() == Message.GET) {
                        qFile.setReaders( -1 );
                        if(qFile.toDelete())
                            fileLock.remove( fileName );
                    }
                    else
                        fileLock.remove( fileName );
                }
            }
        }
    }
    
    /**
     * Checks whether the file is locked or not.
     * 
     * @param fileName
     * @param opType
     * 
     * @return {@code true} if the file is locked,
     *         {@code false} otherwise.
    */
    public boolean getBlocked( final String fileName, final byte opType )
    {
        synchronized ( fileLock ) {
            QuorumFile qFile = fileLock.get( fileName );
            if(qFile == null)
                return false;
            else 
                return (opType != Message.GET || qFile.getOpType() != Message.GET);
        }
    }
}