
package distributed_chord.overlay;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.RMISocketFactory;

import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.utils.ChordUtils;

public class RemoteChordNode extends ChordNode
{
	/** RMI stub node */
	private IChordRMINode node;
	/** RMI stub peer */
	private IChordRMIPeer peer;
	/** Decide if we have to wait for a response */
	//private boolean waitResponse = true;
	
	/** Timeout RMI connection */
	private static final int TIMEOUT_CONNECTION = 1000;
	/** Generated serial ID */
	private static final long serialVersionUID = 3213593327651874990L;
	
	/** Constructor used for the bootstrap node
	 * 
	 * @param address	IP address
	*/
	public RemoteChordNode( final String address ) throws RemoteException, MalformedURLException, NotBoundException
	{
		super();

		setRMIConnection( address );
	}
	
	/** Construct a remote chord node
	 * 
	 * @param ID					node identifier
	 * @param address				node IP address
	 * @param avoidRMIconnection	TRUE to avoid the RMI connection with the node, FALSE otherwise
	*/
	public RemoteChordNode( final long ID, final String address, final boolean avoidRMIconnection )
			throws RemoteException, MalformedURLException, NotBoundException
	{
		super( ID, address );
		
		//String URL = "rmi://" + address + ":" + PORT + "/" + RMIname;
		//node = (IChordRMINode) Naming.lookup( URL );
		if(!avoidRMIconnection)
			setRMIConnection( address );
	}
	
	/** Construct a remote chord node from a JSON object
	 * 
	 * @param j_node				the JSON object
	 * @param avoidRMIconnection	TRUE to avoid the RMI connection with the remote node, FALSE otherwise
	*/
	public RemoteChordNode( final JSONObject j_node, final boolean avoidRMIconnection )
			throws RemoteException, JSONException, MalformedURLException, NotBoundException
	{
		this( j_node.getLong( ChordUtils.JSON_ID ), j_node.getString( ChordUtils.JSON_HOST ), avoidRMIconnection );
	}
	
	/** Set the RMI connection with the remote host
	 * 
	 * @param address	remote IP address
	*/
	private void setRMIConnection( final String address ) throws MalformedURLException, RemoteException, NotBoundException
	{
		try{
			RMISocketFactory.setSocketFactory( new RMISocketFactory()
			{
				public Socket createSocket( String host, int port ) throws IOException
				{
					Socket socket = new Socket();
					//socket.setSoTimeout( TIMEOUT_CONNECTION );
					//socket.setSoLinger( false, 0 );
					socket.connect( new InetSocketAddress( host, port ), TIMEOUT_CONNECTION );
					return socket;
				}
				
				public ServerSocket createServerSocket( int port ) throws IOException
				{
					return new ServerSocket( port );
				}
			});
		}
		catch( Exception e )
		{
			
		}
		
		String URL = "rmi://" + address + ":" + PORT + "/" + RMIname;
		try{ node = (IChordRMINode) Naming.lookup( URL ); }
		catch( MalformedURLException | RemoteException | NotBoundException e ){}
		
		/*Timer timer = new Timer( TIMEOUT_CONNECTION, new ActionListener(){
			@Override
			public void actionPerformed( ActionEvent e )
			{
				waitResponse = false;
			}
		} );
		
		new Thread(){
			@Override
			public void run()
			{
				String URL = "rmi://" + address + ":" + PORT + "/" + RMIname;
				try{ node = (IChordRMINode) Naming.lookup( URL ); }
				catch( MalformedURLException | RemoteException | NotBoundException e ){}
				
				waitResponse = false;
			}
		}.start();
		
		timer.start();
		
		while(waitResponse)
		{
			try{ Thread.sleep( 500 );}
			catch( InterruptedException e1 ){}
		}
		
		timer.stop();*/
		
		if(node == null)
			throw new RemoteException();
	}

	/** Return the RMI node stub */
	public IChordRMINode getNode()
	{
		return node;
	}
	
	/** Return the RMI stub peer */
	public IChordRMIPeer getPeer()
	{
		return peer;
	}
	
	/** Set the RMI stub peer
	 * 
	 * @param peer	the RMI stub peer
	*/
	public void setPeer( IChordRMIPeer peer )
	{
		this.peer = peer;
	}
	
	@Override
	public String toString()
	{
		return "{" + ID + ":" + address + "}";
	}
}