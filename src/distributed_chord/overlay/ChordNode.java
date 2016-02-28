/**
 * @author Stefano Ceccotti
*/

package distributed_chord.overlay;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.log4j.BasicConfigurator;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.utils.ChordUtils;

/** Abstract class representing a Chord node */
public abstract class ChordNode extends UnicastRemoteObject implements Serializable
{
	/** Node ID */
	protected long ID;
	/** Address of the node */
	protected String address;
	
	/** RMI name address */
	protected static final String RMIname = "ChordNode";
	/** Port used for the RMI communications */
	protected static final int PORT = 6535;
	
	/** Generated serial ID */
	private static final long serialVersionUID = 596250732494444335L;
	
	public ChordNode() throws RemoteException
	{
		
	}
	
	/** Construct a new Chord node getting the network address.
	 *  
	 *  @param inet	the specified network interface
	*/
	public ChordNode( String inet ) throws IOException
	{
		// set up a simple configuration that logs on the console
		BasicConfigurator.configure();
		
		boolean close = false;
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netint : Collections.list( nets ))
		{
			String net = netint.getName();
			//System.out.println( "Name: " + net );
			if(inet == null && (net.contains( "eth" ) || net.contains( "wlan" )) || net.equals( inet ))
			{
				Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
				for(InetAddress inetAddress : Collections.list( inetAddresses ))
				{
					String address = inetAddress.getHostAddress();
					if(!address.contains( ":" ))
					{
						this.address = address;
						close = true;
						break;
					}
					//System.out.println( "InetAddress: " + address );
				}
			}
			
			if(close)
				break;
		}
		
		if(address == null)
			throw new IOException( "IP address not found: check your Internet connection" );
		
		ChordUtils.LOGGER.info( "ADDRESS: " + address );
		
		//if(System.getSecurityManager() == null)
			//System.setSecurityManager( new SecurityManager() );
		
		// create the RMI registry
		System.setProperty( "java.rmi.server.hostname", address );
		ChordNode objServer = this;
		Registry reg = LocateRegistry.createRegistry( PORT );
		reg.rebind( RMIname, objServer );
	}
	
	public ChordNode( final long ID, final String address )  throws RemoteException
	{
		this.ID = ID;
		this.address = address;
	}
	
	/** Return the IP address of the node */
	public String getAddress()
	{
		return address;
	}
	
	/** Return the ID of the node */
	public long getID()
	{
		return ID;
	}
	
	/** Return the JSON representation of the node */
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject node = new JSONObject();
		
		node.put( ChordUtils.JSON_HOST, address );
		node.put( ChordUtils.JSON_ID, ID );
		
		return node;
	}
	
	@Override
	public String toString()
	{
		return "{" + ID + ":" + address + "}";
	}
}