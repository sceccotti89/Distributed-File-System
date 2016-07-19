/**
 * @author Stefano Ceccotti
*/

package client.manager;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.utils.resources.ResourceLoader;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public abstract class DFSManager
{
	protected TCPnet net;
	protected String address;
	protected int port;
	protected AtomicBoolean closed = new AtomicBoolean( false );
	
	protected boolean useLoadBalancer;
	protected List<GossipMember> loadBalancers;
	protected String destId;
	
    protected final ConsistentHasher<GossipMember, String> cHasher;
    protected ClientMembershipManagerThread membMgr_t;
	
	private static final String DISTRIBUTED_FS_CONFIG = "Settings/ClientSettings.json";
	protected static final Logger LOGGER = Logger.getLogger( DFSManager.class );
	
	
	
	public DFSManager( final String ipAddress,
	                   final int port,
					   final boolean useLoadBalancer,
					   final List<GossipMember> members ) throws IOException, DFSException
	{
	    this.useLoadBalancer = useLoadBalancer;
	    cHasher = new ConsistentHasherImpl<>();
	    
		setConfigure( ipAddress, port, members );
	}
	
	/** 
	 * Sets the initial configuration.
	 * 
	 * @param ipAddress  the ip address
	 * @param port       the net port
	 * @param members    list of initial members
	 * 
	 * @throws DFSException    when something wrong during the configuration.
	*/
	private void setConfigure( final String ipAddress, final int port,
	                           final List<GossipMember> members )
	                                   throws IOException, DFSException
	{
		if(!DFSUtils.initConfig) {
		    DFSUtils.initConfig = true;
		    PropertyConfigurator.configure( ResourceLoader.getResourceAsStream( DFSUtils.LOG_CONFIG ) );
			BasicConfigurator.configure();
		}
		
		LOGGER.info( "Starting the system..." );
		
		address = ipAddress;
		JSONObject file = null;
		// Read the informations from the configuration file.
		try {
    		file = DFSUtils.parseJSONFile( DISTRIBUTED_FS_CONFIG );
    		if(address == null) {
    		    JSONArray inetwork = file.getJSONArray( "network_interface" );
        		String inet = inetwork.getJSONObject( 0 ).getString( "type" );
        		int IPversion = inetwork.getJSONObject( 1 ).getInt( "IPversion" );
        		address = getNetworkAddress( inet, IPversion );
    		}
		}
		catch( IOException e ) {
		    // The file is not found or not well-structured.
		    throw e;
		}
        
		this.port = (port <= 0) ? DFSUtils.SERVICE_PORT : port;
		
		net = new TCPnet( address, this.port );
        net.setSoTimeout( 5000 );
        
        if(!useLoadBalancer)
            membMgr_t = new ClientMembershipManagerThread( net, this, cHasher );
		
		if(members != null) {
		    // Get the remote nodes from the input list.
			loadBalancers = new ArrayList<>( members.size() );
			
			for(GossipMember member : members) {
				if(member.getNodeType() == GossipMember.LOAD_BALANCER)
				    loadBalancers.add( member );
				else {
				    String id = DFSUtils.getNodeId( 1, member.getAddress() );
	                member.setId( id );
				    cHasher.addBucket( member, 1 );
				}
				
				LOGGER.debug( "Added remote node: " + member.getAddress() + ":" + member.getNodeType() );
			}
		}
		else {
			if(file != null) {
			    // Get the remote nodes from the configuration file.
			    JSONArray nodes = file.getJSONArray( "members" );
    			loadBalancers = new ArrayList<>( nodes.length() );
    			for(int i = nodes.length()-1; i >= 0; i--) {
    				JSONObject data = nodes.getJSONObject( i );
    				int nodeType = data.getInt( "nodeType" );
    				String address = data.getString( "host" );
    				int Port = data.getInt( "port" );
    				String id = DFSUtils.getNodeId( 1, address + ":" + port );
    				GossipMember node = new RemoteGossipMember( address, Port, id, 0, nodeType );
    				if(nodeType == GossipMember.LOAD_BALANCER)
    				    loadBalancers.add( node );
    				else
    				    cHasher.addBucket( node, 1 );
    				
    				LOGGER.debug( "Added remote node: " + node.getAddress() + ":" + nodeType );
    			}
		    }
		}
		
		// Some checks...
		if(!useLoadBalancer && cHasher.isEmpty()) {
		    LOGGER.info( "The list of nodes is empty. We are now using LoadBalancer nodes." );
		    setUseLoadBalancers( true );
		}
	}
	
	private String getNetworkAddress( final String inet, final int IPversion ) throws IOException
	{
		String _address = null;
		// Enumerate all the network interfaces.
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netint : Collections.list( nets )) {
			if(netint.getName().equals( inet )) {
				// Enumerate all the IP address associated with it.
				for(InetAddress inetAddress : Collections.list( netint.getInetAddresses() )) {
					if(!inetAddress.isLoopbackAddress() &&
					  ((IPversion == 4 && inetAddress instanceof Inet4Address) ||
					  (IPversion == 6 && inetAddress instanceof Inet6Address))) {
						_address = inetAddress.getHostAddress();
						if(inetAddress instanceof Inet6Address) {
							int index = _address.indexOf( '%' );
							_address = _address.substring( 0, index );
						}
						
						break;
					}
				}
			}
			
			if(_address != null)
				break;
		}
		
		if(_address == null) {
			throw new IOException( "IP address not found: check your Internet connection or the configuration file " +
									DISTRIBUTED_FS_CONFIG );
		}
		
		LOGGER.info( "Address: " + _address );
		
		return _address;
	}
	
	/**
     * Enable/disable the using of LoadBalancer nodes.
     * 
     * @param useLB    {@code true} to enable the load balancer usage,
     *                 {@code false} to disable it
    */
    public void setUseLoadBalancers( final boolean useLB )
    {
        if(useLoadBalancer == useLB)
            return;
        useLoadBalancer = useLB;
        
        if(!useLoadBalancer) {
            membMgr_t = new ClientMembershipManagerThread( net, this, cHasher );
            membMgr_t.start();
        }
        else {
            // Close the background thread.
            if(membMgr_t != null) {
                membMgr_t.close();
                try { membMgr_t.join(); }
                catch( InterruptedException e ) {}
            }
        }
    }
	
	protected void sendPutMessage( final TCPSession session, final DistributedFile file, final String hintedHandoff ) throws IOException
	{
	    LOGGER.info( "Sending data..." );
		
	    MessageRequest message;
		if(useLoadBalancer) {
		    message = new MessageRequest( Message.PUT, file.getName(), file.read() );
		    message.putMetadata( address + ":" + port, null );
		}
		else {
	        Metadata meta = new Metadata( null, hintedHandoff );
	        message = new MessageRequest( Message.PUT, file.getName(), file.read(), true, destId, meta );
		}
		
		session.sendMessage( message, true );
		LOGGER.info( "Data sent." );
	}
	
	protected void sendGetMessage( final TCPSession session, final String fileName ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		
		MessageRequest message;
		if(useLoadBalancer) {
            message = new MessageRequest( Message.GET, fileName );
            message.putMetadata( address + ":" + port, null );
		}
        else {
            Metadata meta = new Metadata( null, null );
            message = new MessageRequest( Message.GET, fileName, null, true, destId, meta );
        }
		
		session.sendMessage( message, true );
		LOGGER.info( "Data sent." );
	}
	
	protected List<DistributedFile> readGetResponse( final TCPSession session ) throws IOException
    {
	    return readGetAllResponse( session );
    }
	
	protected void sendGetAllMessage( final TCPSession session, final String fileName ) throws IOException
    {
	    MessageRequest message;
	    
        LOGGER.info( "Sending data..." );
        if(useLoadBalancer) {
            message = new MessageRequest( Message.GET_ALL, fileName );
            message.putMetadata( address + ":" + port, null );
        }
        else {
            Metadata meta = new Metadata( null, null );
            message = new MessageRequest( Message.GET_ALL, fileName, null, false, null, meta );
        }
        
        session.sendMessage( message, true );
        LOGGER.info( "Data sent." );
    }
	
	protected List<DistributedFile> readGetAllResponse( final TCPSession session ) throws IOException
	{
	    LOGGER.info( "Waiting for the incoming files..." );
	    
        ByteBuffer msg = ByteBuffer.wrap( session.receive() );
	    int numFiles = msg.getInt();
	    List<DistributedFile> files = new ArrayList<>( numFiles );
	    if(numFiles > 0) {
	        for(int i = 0; i < numFiles; i++) {
	            DistributedFile file = new DistributedFile( session.receive() );
                LOGGER.debug( "File \"" + file.getName() + "\" downloaded." );
                files.add( file );
	        }
	    }
        
        LOGGER.info( "Received " + files.size() + " files." );
        
        return files;
	}
	
	protected void sendDeleteMessage( final TCPSession session, final DistributedFile file, final String hintedHandoff ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		
		MessageRequest message;
        if(useLoadBalancer) {
            message = new MessageRequest( Message.DELETE, file.getName(), file.read() );
            message.putMetadata( address + ":" + port, null );
        }
        else {
            Metadata meta = new Metadata( null, hintedHandoff );
            message = new MessageRequest( Message.DELETE, file.getName(), file.read(), true, destId, meta );
        }
		
        session.sendMessage( message, true );
		
		LOGGER.info( "Data sent." );
	}
	
	protected boolean checkResponse( final TCPSession session, final String op, final boolean toPrint ) throws IOException
	{
		MessageResponse message = session.receiveMessage();
		byte opType = message.getType();
		if(toPrint)
			LOGGER.debug( "Received: " + getStringCode( opType ) );
		
		return (opType != Message.TRANSACTION_FAILED);
	}
	
	private String getStringCode( final byte code )
	{
		if(code == Message.TRANSACTION_OK)
			return "TRANSACTION OK";
		else
			return "TRANSACTION FAILED";
	}
	
	public void shutDown()
	{
	    closed.set( true );
	    
	    if(membMgr_t != null) {
	        membMgr_t.close();
    	    try { membMgr_t.join(); }
    	    catch( InterruptedException e ) {}
	    }
	}
}
