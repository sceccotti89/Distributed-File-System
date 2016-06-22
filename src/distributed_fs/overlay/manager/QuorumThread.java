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
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.Timer;

import org.json.JSONException;

import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.overlay.DFSNode;
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
    private boolean shutDown;
    
    private Timer timer;
    private long nextId = -1L;
    private long id = -1L;
    private final Map<String, QuorumFile> fileLock = new HashMap<>( 64 );
    
    private static final ReentrantLock QUORUM_LOCK = new ReentrantLock( true );
	
    private static final short BLOCKED_TIME = 10000; // 10 seconds.
    private static final byte MAKE_QUORUM = 0, RELEASE_QUORUM = 1;
    private static final byte ACCEPT_QUORUM_REQUEST = 0, DECLINE_QUORUM_REQUEST = 1;
    
    public QuorumThread( final int port,
                         final String address,
                         final DFSNode node ) throws IOException
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
        /*UDPnet net;
        
        //try{ net = new UDPnet( _address, QUORUM_PORT ); }
        try{ net = new UDPnet( _address, port ); }
        catch( IOException e ) {
            return;
        }*/
        
        // TODO utilizzare RUDP, perche' cosi' e' troppo costoso
        TCPnet net;
        
        //try{ net = new UDPnet( _address, QUORUM_PORT ); }
        try{
            net = new TCPnet( address, port );
            net.setSoTimeout( DFSNode.WAIT_CLOSE );
        }
        catch( IOException e ) {
            e.printStackTrace();
            return;
        }
        
        //System.out.println( "[QUORUM] Waiting on " + address + ":" + port );
        
        byte[] msg;
        while(!shutDown) {
            try {
                TCPSession session = net.waitForConnection();
                if(session == null)
                    continue;
                
                // Read the request.
                ByteBuffer data = ByteBuffer.wrap( session.receiveMessage() );
                DFSNode.LOGGER.info( "[QUORUM] Received a connection from: " + session.getSrcAddress() );
                
                switch( data.get() ) {
                    case( MAKE_QUORUM ):
                        byte opType = data.get();
                        String fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        boolean locked = setLocked( true, fileName, 0, opType );
                        
                        DFSNode.LOGGER.info( "Received a MAKE_QUORUM request for '" + fileName +
                                             "'. Request status: " + (!locked ? "BLOCKED" : "FREE") );
                        
                        // Send the current blocked state.
                        if(!locked)
                            msg = new byte[]{ DECLINE_QUORUM_REQUEST };
                        else 
                            msg = net.createMessage( new byte[]{ ACCEPT_QUORUM_REQUEST }, DFSUtils.longToByteArray( id ), false );
                        session.sendMessage( msg, false );
                        
                        break;
                    
                    case( RELEASE_QUORUM ):
                        DFSNode.LOGGER.info( "Received a RELEASE_QUORUM request" );
                        long id = data.getLong();
                        fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
                        setLocked( false, fileName, id, (byte) 0x0 ); // Here the operation type is useless.
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
     * @param quorum    
     * @param opType    
     * @param fileName  
     * @param destId    
     * 
     * @return list of contacted nodes, that have agreed to the quorum
    */
    public List<QuorumNode> checkQuorum( final TCPSession session,
    									 final QuorumSession quorum,
                                         final byte opType,
                                         final String fileName,
                                         final String destId ) throws IOException
    {
        List<GossipMember> nodes = node.getSuccessorNodes( destId, address, QuorumSession.getMaxNodes() );
        
        DFSNode.LOGGER.debug( "Neighbours: " + nodes.size() );
        if(nodes.size() < QuorumSession.getMinQuorum( opType )) {
            DFSNode.LOGGER.info( "[SN] Quorum failed: " + nodes.size() + "/" + QuorumSession.getMinQuorum( opType ) );
            
            // If there is a number of nodes less than the quorum,
            // we neither start the protocol.
            sendQuorumResponse( session, Message.TRANSACTION_FAILED );
            return new ArrayList<>();
        }
        else {
            List<QuorumNode> nodeAddress = contactNodes( session, quorum, opType, fileName, nodes );
            return nodeAddress;
        }
    }

    /**
     * Contacts the nodes to complete the quorum phase.
     * 
     * @param session   
     * @param quorum    
     * @param opType    
     * @param fileName  
     * @param nodes     
     * 
     * @return list of contacted nodes, that have agreed to the quorum
    */
    private List<QuorumNode> contactNodes( final TCPSession session,
    									   final QuorumSession quorum,
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
            DFSNode.LOGGER.info( "[SN] Contacting " + node + "..." );
            TCPSession mySession = null;
            try {
                mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
                byte[] msg = net.createMessage( new byte[]{ MAKE_QUORUM, opType }, fileName.getBytes( StandardCharsets.UTF_8 ), true );
                mySession.sendMessage( msg, true );
                
                DFSNode.LOGGER.info( "[SN] Waiting the response..." );
                ByteBuffer data = ByteBuffer.wrap( mySession.receiveMessage() );
                mySession.close();
                
                if(data.get() == ACCEPT_QUORUM_REQUEST) {
                    DFSNode.LOGGER.info( "[SN] Node " + node + " agree to the quorum." );
                    // Not blocked => agree to the quorum.
                    QuorumNode qNode = new QuorumNode( quorum, node, fileName, opType, data.getLong() );
                    qNode.addAgreedNodes( agreedNodes );
                    agreedNodes.add( qNode );
                    quorum.saveState( agreedNodes );
                }
                else {
                    // Blocked => the node doesn't agree to the quorum.
                    DFSNode.LOGGER.info( "[SN] Node " + node + " doesn't agree to the quorum." );
                    if(QuorumSession.unmakeQuorum( ++errors, opType )) {
                        cancelQuorum( session, quorum, agreedNodes );
                        break;
                    }
                }
            }
            catch( IOException | JSONException e ) {
                // Ignored.
                //e.printStackTrace();
                
                if(mySession != null)
                    mySession.close();
                
                DFSNode.LOGGER.info( "[SN] Node " + node + " is not reachable." );
                if(QuorumSession.unmakeQuorum( ++errors, opType )) {
                    cancelQuorum( session, quorum, agreedNodes );
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
     * @param quorum        the actual quorum session
     * @param agreedNodes   list of contacted nodes
    */
    public void cancelQuorum( final TCPSession session, final QuorumSession quorum, final List<QuorumNode> agreedNodes ) throws IOException
    {
        if(session != null)
            DFSNode.LOGGER.info( "[SN] The quorum cannot be reached. The transaction will be closed." );
        
        closeQuorum( quorum, agreedNodes );
        // send to the client the negative response
        sendQuorumResponse( session, Message.TRANSACTION_FAILED );
    }
    
    /**
     * Close an opened quorum.
     * 
     * @param quorum        
     * @param agreedNodes   
    */
    public void closeQuorum( final QuorumSession quorum, final List<QuorumNode> agreedNodes )
    {
        //UDPnet net = new UDPnet();
        TCPnet net = new TCPnet();
        //System.out.println( "[SN] NODES:" + agreedNodes );
        for(int i = agreedNodes.size() - 1; i >= 0; i--) {
            GossipMember node = agreedNodes.get( i ).getNode();
            TCPSession mySession = null;
            try {
                mySession = net.tryConnect( node.getHost(), node.getPort() + 3 );
                byte[] msg = net.createMessage( new byte[]{ RELEASE_QUORUM },
                                                DFSUtils.longToByteArray( agreedNodes.get( i ).getId() ),
                                                false );
                msg = net.createMessage( msg, agreedNodes.get( i ).getFileName().getBytes( StandardCharsets.UTF_8 ), true );
                mySession.sendMessage( msg, true );
                
                agreedNodes.remove( i );
                quorum.saveState( agreedNodes );
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
     * @param toLock    {@code true} to (try to) lock the file, {@code false} otherwise
     * @param fileName  name of the file
     * @param fileId    id of the quorum file
     * @param opType    operation type
     * 
     * @return {@code true} if the file has been locked, {@code false} otherwise.
    */
    public boolean setLocked( final boolean toLock, final String fileName, final long fileId, final byte opType )
    {
        boolean isLocked = true;
        
        QUORUM_LOCK.lock();
    	
        QuorumFile qFile = fileLock.get( fileName );
        
        if(toLock) {
            if(qFile != null) {
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
            if(qFile != null && fileId == qFile.getId()) {
                if(qFile.getOpType() == Message.GET) {
                    qFile.setReaders( -1 );
                    if(qFile.toDelete())
                        fileLock.remove( fileName );
                }
                else
                    fileLock.remove( fileName );
            }
        }
        
        QUORUM_LOCK.unlock();
        
        return isLocked;
    }
    
    /**
	 * Class used to manage the agreed nodes of the quorum.
	*/
	public static class QuorumNode
	{
		private final QuorumSession quorum;
		private final GossipMember node;
		private List<QuorumNode> nodes;
		private final String fileName;
		private final byte opType;
		private final long id;
		
		public QuorumNode( final QuorumSession quorum, final GossipMember node,
						   final String fileName, final byte opType, final long id )
		{
			this.quorum = quorum;
			this.node = node;
			this.fileName = fileName;
			this.opType = opType;
			this.id = id;
		}
		
		public QuorumSession getQuorum() {
			return quorum;
		}
		
		public GossipMember getNode() {
			return node;
		}
		
		/**
		 * Method used, during the transmission of the files,
		 * to set the list of agreed nodes.
		*/
		public void addAgreedNodes( final List<QuorumNode> nodes ) {
			this.nodes = nodes;
		}
		
		public List<QuorumNode> getList() {
			return nodes;
		}
		
		public String getFileName() {
			return fileName;
		}
		
		public byte getOpType() {
			return opType;
		}
		
		public long getId() {
			return id;
		}
	}
	
	/**
	 * Class used to represent a file during the quorum phase.
	 * The object remains in the Map as long as its TimeToLive
	 * is greater than 0.
	*/
	public static class QuorumFile
	{
		/** Maximum waiting time of the file in the Map. */
		private static final long MAX_TTL = 60000; // 1 Minute.
		
		private long id;
		private long ttl = MAX_TTL;
		private byte opType;
		private int readers = 0;
		
		public QuorumFile( final long id, final byte opType )
		{
			this.id = id;
			this.opType = opType;
			if(opType == Message.GET)
				readers = 1;
		}
		
		public void updateTTL( final int delta )
		{
			ttl -= delta;
		}
		
		public boolean toDelete()
		{
			return (opType == Message.GET && readers == 0) || ttl <= 0;
		}
		
		/**
		 * Changes the number of readers.
		 * 
		 * @param value   +1/-1
		*/
		public void setReaders( final int value )
		{
			readers += value;
			if(value == +1) // Restart the time to live.
			    ttl = MAX_TTL;
		}
		
		public byte getOpType()
		{
			return opType;
		}
		
		public long getId()
		{
			return id;
		}
	}
}