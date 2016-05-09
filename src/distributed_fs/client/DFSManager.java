/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.File;
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

import distributed_fs.files.DistributedFile;
import distributed_fs.files.RemoteFile;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.net.messages.MessageResponse;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class DFSManager
{
	protected TCPSession session;
	protected List<GossipMember> loadBalancers;
	protected String _address;
	
	private static final String DISTRIBUTED_FS_CONFIG = "./Settings/ClientSettings.json";
	protected static final Logger LOGGER = Logger.getLogger( DFSManager.class );
	
	public DFSManager( final List<GossipMember> members ) throws IOException, JSONException
	{
		setConfigure( members );
	}
	
	/** 
	 * Sets the initial configuration.
	 * 
	 * @param members	list of initial members
	*/
	protected void setConfigure( final List<GossipMember> members ) throws IOException, JSONException
	{
		BasicConfigurator.configure();
		LOGGER.info( "Starting the system..." );
		
		//java.util.logging.Logger.getLogger( "udt" ).setLevel( java.util.logging.Level.WARNING );
		
		JSONObject file = Utils.parseJSONFile( DISTRIBUTED_FS_CONFIG );
		JSONArray inetwork = file.getJSONArray( "network_interface" );
		String inet = inetwork.getJSONObject( 0 ).getString( "type" );
		int IPversion = inetwork.getJSONObject( 1 ).getInt( "IPversion" );
		_address = this.getNetworkAddress( inet, IPversion );
		
		if(members != null)
			loadBalancers = new ArrayList<>( members );
		else {
			// get the load balancer nodes
			JSONArray nodes = file.getJSONArray( "members" );
			loadBalancers = new ArrayList<>( nodes.length() );
			for(int i = 0; i < nodes.length(); i++) {
				JSONObject data = nodes.getJSONObject( i );
				loadBalancers.add( new RemoteGossipMember( data.getString( "host" ), Utils.SERVICE_PORT, "", 0, GossipMember.STORAGE ) );
				LOGGER.info( "Added load balancer: " + data.getString( "host" ) );
			}
		}
	}
	
	protected String getNetworkAddress( final String inet, final int IPversion ) throws IOException
	{
		String _address = null;
		// enumerate all the network intefaces
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netint : Collections.list( nets )) {
			if(netint.getName().equals( inet )) {
				// enumerate all the IP address associated with it
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
									Utils.DISTRIBUTED_FS_CONFIG );
		
		LOGGER.info( "Address: " + _address );
		
		return _address;
	}
	
	/**
	 * Checks if the file has the correct format.
	 * 
	 * @param fileName	the input file
	 * @param dbRoot	the database root
	*/
	protected String checkFile( String fileName, final String dbRoot )
	{
		//if(!(fileName.startsWith( dbRoot ) || fileName.startsWith( dbRoot.substring( 2 ) )))
			//fileName = dbRoot + fileName;
		
		//if(!fileName.startsWith( "./" ))
			//fileName = "./" + fileName;
		/*if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		if(fileName.startsWith( dbRoot ) || fileName.startsWith( dbRoot.substring( 2 ) )) {
			if(!fileName.startsWith( "./" ))
				fileName = "./" + fileName;
			fileName = fileName.substring( dbRoot.length() );
		}*/
		
		String backup = fileName;
		if(fileName.startsWith( "./" ))
			fileName = fileName.substring( 2 );
		
		File f = new File( fileName );
		fileName = f.getAbsolutePath();
		if(f.isDirectory() && !fileName.endsWith( "/" ))
			fileName += "/";
		fileName = fileName.replace( "\\", "/" ); // System parametric among Windows, Linux and MacOS
		if(fileName.startsWith( dbRoot ))
			fileName = fileName.substring( dbRoot.length() );
		else
			fileName = backup;
		
		return fileName;
	}
	
	protected void sendPutMessage( final RemoteFile file ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		//MessageRequest message = new MessageRequest( Message.PUT, file.getName(), Utils.serializeObject( file ) );
		MessageRequest message = new MessageRequest( Message.PUT, file.getName(), file.read() );
		session.sendMessage( Utils.serializeObject( message ), true );
		LOGGER.info( "Data sent." );
	}
	
	protected void sendGetMessage( final String fileName ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		MessageRequest message = new MessageRequest( Message.GET, fileName );
		session.sendMessage( Utils.serializeObject( message ), true );
		LOGGER.info( "Data sent." );
	}
	
	protected List<RemoteFile> readGetResponse( final TCPSession session ) throws IOException {
		return readGetAllResponse( session );
	}
	
	protected void sendGetAllMessage() throws IOException
	{
		LOGGER.info( "Sending data..." );
		MessageRequest message = new MessageRequest( Message.GET_ALL );
		session.sendMessage( Utils.serializeObject( message ), true );
		LOGGER.info( "Data sent." );
	}
	
	protected List<RemoteFile> readGetAllResponse( final TCPSession session ) throws IOException
	{
		LOGGER.info( "Waiting for the incoming files..." );
		
		MessageResponse message = Utils.deserializeObject( session.receiveMessage() );
		List<RemoteFile> files = new ArrayList<>();
		if(message.getFiles() != null) {
			for(byte[] file : message.getFiles())
				//files.add( Utils.deserializeObject( file ) );
				files.add( new RemoteFile( file ) );
		}
		
		LOGGER.info( "Received " + files.size() + " files." );
		
		return files;
	}
	
	protected void sendDeleteMessage( final DistributedFile file ) throws IOException
	{
		LOGGER.info( "Sending data..." );
		//MessageRequest message = new MessageRequest( Message.DELETE, file.getName(), Utils.serializeObject( file ) );
		MessageRequest message = new MessageRequest( Message.DELETE, file.getName(), file.read() );
		session.sendMessage( Utils.serializeObject( message ), true );
		LOGGER.info( "Data sent." );
	}
	
	protected boolean checkResponse( final TCPSession session, final String op, final boolean toPrint ) throws IOException
	{
		MessageResponse message = Utils.deserializeObject( session.receiveMessage() );
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
}