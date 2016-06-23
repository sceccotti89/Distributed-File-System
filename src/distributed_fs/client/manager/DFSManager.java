/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client.manager;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.net.messages.Metadata;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.RemoteFile;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public abstract class DFSManager
{
	protected TCPnet net;
	protected TCPSession session;
	protected String address;
	protected int port;
	protected boolean closed = false;
	
	protected boolean useLoadBalancer;
	protected List<GossipMember> loadBalancers;
	protected String destId;
	
    protected ConsistentHasherImpl<GossipMember, String> cHasher;
    protected MembershipManagerThread listMgr_t;
	
	private static final String DISTRIBUTED_FS_CONFIG = "./Settings/ClientSettings.json";
	protected static final Logger LOGGER = Logger.getLogger( DFSManager.class );
	
	public DFSManager( final String ipAddress,
	                   final int port,
					   final boolean useLoadBalancer,
					   final List<GossipMember> members ) throws IOException, JSONException, DFSException
	{
	    this.useLoadBalancer = useLoadBalancer;
	    
		setConfigure( ipAddress, port, members );
	}
	
	/** 
	 * Sets the initial configuration.
	 * 
	 * @param ipAddress  the ip address
	 * @param port       the net port
	 * @param members    list of initial members
	 * 
	 * @throws DFSException if something wrong during the configuration.
	*/
	private void setConfigure( final String ipAddress, final int port,
	                           final List<GossipMember> members )
	                                   throws IOException, JSONException, DFSException
	{
		if(!DFSUtils.initConfig) {
		    DFSUtils.initConfig = true;
			BasicConfigurator.configure();
		}
		
		LOGGER.info( "Starting the system..." );
		
		//java.util.logging.Logger.getLogger( "udt" ).setLevel( java.util.logging.Level.WARNING );
		
		JSONObject file = DFSUtils.parseJSONFile( DISTRIBUTED_FS_CONFIG );
		JSONArray inetwork = file.getJSONArray( "network_interface" );
		String inet = inetwork.getJSONObject( 0 ).getString( "type" );
		int IPversion = inetwork.getJSONObject( 1 ).getInt( "IPversion" );
		address = ipAddress;
		if(address == null)
			address = getNetworkAddress( inet, IPversion );
		this.port = (port <= 0) ? DFSUtils.SERVICE_PORT : port;
		
		net = new TCPnet( address, this.port );
        net.setSoTimeout( 5000 );
        
        if(!useLoadBalancer) {
            cHasher = new ConsistentHasherImpl<>();
            listMgr_t = new MembershipManagerThread( net, cHasher );
        }
		
		if(members != null) {
		    // Get the remote nodes from the input list.
			if(useLoadBalancer)
		        loadBalancers = new ArrayList<>( members.size() );
			
			for(GossipMember member : members) {
				if(useLoadBalancer && member.getNodeType() == GossipMember.LOAD_BALANCER)
					loadBalancers.add( new RemoteGossipMember( member.getHost(), member.getPort(),
					                                           DFSUtils.getNodeId( 1, member.getHost() ),
					                                           0, member.getNodeType() ) );
				else {
				    if(!useLoadBalancer && member.getNodeType() == GossipMember.STORAGE) {
    				    GossipMember _member = new RemoteGossipMember( member.getHost(), member.getPort(),
    				                                                   DFSUtils.getNodeId( 1, member.getHost() ),
    				                                                   0, member.getNodeType() );
    				    cHasher.addBucket( _member, 1 );
				    }
				}
			}
		}
		else {
			// Get the load balancer nodes from the configuration file.
			JSONArray nodes = file.getJSONArray( "members" );
			loadBalancers = new ArrayList<>( nodes.length() );
			for(int i = 0; i < nodes.length(); i++) {
				JSONObject data = nodes.getJSONObject( i );
				int nodeType = data.getInt( "nodeType" );
				String address = data.getString( "host" );
				if(useLoadBalancer && nodeType == GossipMember.LOAD_BALANCER)
				    loadBalancers.add( new RemoteGossipMember( address, DFSUtils.SERVICE_PORT, DFSUtils.getNodeId( 1, address ), 0, nodeType ) );
				else {
				    if(!useLoadBalancer && nodeType == GossipMember.STORAGE) {
    				    GossipMember member = new RemoteGossipMember( address, DFSUtils.SERVICE_PORT, DFSUtils.getNodeId( 1, address ), 0, nodeType );
    				    cHasher.addBucket( member, 1 );
				    }
				}
				
				LOGGER.info( "Added remote node: " + data.getString( "host" ) );
			}
		}
		
		// Some checks...
		if((!useLoadBalancer && cHasher.getSize() == 0) ||
		    (useLoadBalancer && loadBalancers.isEmpty()))
		    throw new DFSException( "The list of members cannot be empty." );
	}
	
	private String getNetworkAddress( final String inet, final int IPversion ) throws IOException
	{
		String _address = null;
		// Enumerate all the network intefaces.
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
		
		if(_address == null)
			throw new IOException( "IP address not found: check your Internet connection or the configuration file " +
									DFSUtils.DISTRIBUTED_FS_CONFIG );
		
		LOGGER.info( "Address: " + _address );
		
		return _address;
	}
	
	protected void sendPutMessage( final RemoteFile file, final String hintedHandoff ) throws IOException
	{
	    LOGGER.info( "Sending data..." );
		
	    MessageRequest message;
		if(useLoadBalancer) {
		    message = new MessageRequest( Message.PUT, file.getName(), file.read() );
		    message.putMetadata( address + ":" + port, null );
		}
		else {
		    //String destId = DFSUtils.getId( file.getName() );
	        Metadata meta = new Metadata( null, hintedHandoff );
	        message = new MessageRequest( Message.PUT, file.getName(), file.read(), true, destId, meta );
		}
		
		session.sendMessage( message, true );
		LOGGER.info( "Data sent." );
	}
	
	protected void sendGetMessage( final String fileName ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		
		MessageRequest message;
		if(useLoadBalancer) {
            message = new MessageRequest( Message.GET, fileName );
            message.putMetadata( address + ":" + port, null );
		}
        else {
            //String destId = DFSUtils.getId( fileName );
            Metadata meta = new Metadata( null, null );
            message = new MessageRequest( Message.GET, fileName, null, true, destId, meta );
        }
		
		session.sendMessage( message, true );
		LOGGER.info( "Data sent." );
	}
	
	protected List<RemoteFile> readGetResponse( final TCPSession session ) throws IOException {
        return readGetAllResponse( session );
    }
	
	protected void sendGetAllMessage( final String fileName ) throws IOException
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
	
	protected List<RemoteFile> readGetAllResponse( final TCPSession session ) throws IOException
	{
	    LOGGER.info( "Waiting for the incoming files..." );
        
        MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
        List<RemoteFile> files = new ArrayList<>();
        List<byte[]> objects = message.getObjects();
        if(objects != null) {
            for(byte[] file : objects)
                //files.add( Utils.deserializeObject( file ) );
                files.add( new RemoteFile( file ) );
        }
        
        LOGGER.info( "Received " + files.size() + " files." );
        
        return files;
	}
	
	protected void sendDeleteMessage( final DistributedFile file, final String hintedHandoff ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		
		MessageRequest message;
        if(useLoadBalancer) {
            //message = new MessageRequest( Message.DELETE, file.getName(), file.read() );
            message = new MessageRequest( Message.DELETE, file.getName(), DFSUtils.serializeObject( file ) );
            message.putMetadata( address + ":" + port, null );
        }
        else {
            //String destId = DFSUtils.getId( file.getName() );
            Metadata meta = new Metadata( null, hintedHandoff );
            //message = new MessageRequest( Message.DELETE, file.getName(), file.read(), true, destId, meta );
            message = new MessageRequest( Message.DELETE, file.getName(), DFSUtils.serializeObject( file ), true, destId, meta );
        }
		
        session.sendMessage( message, true );
		
		LOGGER.info( "Data sent." );
	}
	
	protected boolean checkResponse( final TCPSession session, final String op, final boolean toPrint ) throws IOException
	{
		MessageResponse message = DFSUtils.deserializeObject( session.receiveMessage() );
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
	    closed = true;
	    if(listMgr_t != null) {
    	    listMgr_t.close();
    	    // TODO fare la join sul thread?
	    }
	}
}