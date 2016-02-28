/**
 * @author Stefano Ceccotti
*/

package distributed_chord.manager;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.overlay.ChordPeer;
import distributed_chord.overlay.IChordRMIPeer;
import distributed_chord.overlay.LocalChordNode;
import distributed_chord.overlay.RemoteChordNode;
import distributed_chord.utils.ChordUtils;
import distributed_chord.utils.HashFunction;

public class ChordManager implements IChordManager
{
	/** Associated peer node */
	private ChordPeer peer;
	
	public ChordManager( String args[] ) throws IOException, NotBoundException, NoSuchAlgorithmException
	{
		HashFunction _hash = new HashFunction( HashFunction._HASH.SHA_256 );
		
		String inet = null;
		
		int length = args.length;
		if(length > 0)
			inet = args[0];
		
		peer = new ChordPeer( (short) 3, _hash, inet );
	}
	
	/** Start the Chord system */
	public void start() throws IOException, JSONException, NotBoundException
	{
		peer.join();
	}
	
	@Override
	public void get( final List<String> files ) throws JSONException, NotBoundException, IOException
	{
		int size = files.size();
		HashMap<Long, List<String>> map = getNodeMap( files, size );
		
		LocalChordNode _node = peer.getNode();
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.getID(), ID );
			if(s_node != null)
			{
				List<String> data = map.get( ID );
				
				JSONObject j_node = new JSONObject( s_node );
				long hostID = j_node.getLong( ChordUtils.JSON_ID );
				
				if(_node.containsKey( hostID ))
				{
					// if the node is local, just compute the retrieve operation
					if(!peer.retrieve( ID, data ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.getFingerTable(), _node.getSuccessors() );
	
				try{
					IChordRMIPeer _peer = node.getPeer();
					if(_peer == null)
					{
						_peer = createChordPeer( node.getAddress(), peer.getPort(), peer.getRMIName() );
						node.setPeer( _peer );
					}
					
					if(!_peer.retrieve( ID, data ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.getAddress() + " not reachable" );
				}
			}
			else
				System.out.println( "No corresponding node is founded for the ID " + ID );
		}
	}
	
	@Override
	public void put( final List<String> files ) throws JSONException, IOException, NotBoundException
	{
		int size = files.size();
		HashMap<Long, List<String>> map = getNodeMap( files, size );
		
		LocalChordNode _node = peer.getNode();
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.getID(), ID );
			if(s_node != null)
			{
				List<String> data = map.get( ID );
				size = data.size();
				
				// create the list of bytes
				List<byte[]> bytes = new ArrayList<>( size );
				for(int i = 0; i < size; i++)
					bytes.add( ChordUtils.serializeObject( ChordUtils.RESOURCE_LOCATION + data.get( i ) ) );
				
				JSONObject j_node = new JSONObject( s_node );
				long hostID = j_node.getLong( ChordUtils.JSON_ID );
				
				if(_node.containsKey( hostID ))
				{
					// if the node is local, just compute the store operation
					if(!peer.store( data, bytes ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.getFingerTable(), _node.getSuccessors() );
	
				try{
					IChordRMIPeer _peer = node.getPeer();
					if(_peer == null)
					{
						_peer = createChordPeer( node.getAddress(), peer.getPort(), peer.getRMIName() );
						node.setPeer( _peer );
					}
					
					if(!_peer.store( data, bytes ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.getAddress() + " not reachable" );
				}
			}
			else
				System.out.println( "No corresponding node is founded for the ID " + ID );
		}
	}
	
	@Override
	public void delete( final List<String> files ) throws JSONException, NotBoundException, IOException
	{
		int size = files.size();
		HashMap<Long, List<String>> map = getNodeMap( files, size );
		
		LocalChordNode _node = peer.getNode();
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.getID(), ID );
			if(s_node != null)
			{
				List<String> data = map.get( ID );
				
				JSONObject j_node = new JSONObject( s_node );
				long hostID = j_node.getLong( ChordUtils.JSON_ID );
				
				if(_node.containsKey( hostID ))
				{
					// if the node is local, just compute the delete operation
					if(!peer.remove( data ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.getFingerTable(), _node.getSuccessors() );
	
				try{
					IChordRMIPeer _peer = node.getPeer();
					if(_peer == null)
					{
						//String URL = "rmi://" + node.getAddress() + ":" + peer.getPort() + "/" + peer.getRMIName();
						//_peer = (IChordRMIPeer) Naming.lookup( URL );
						_peer = createChordPeer( node.getAddress(), peer.getPort(), peer.getRMIName() );
						node.setPeer( _peer );
					}
					
					if(!_peer.remove( data ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.getAddress() + " not reachable" );
				}
			}
			else
				System.out.println( "No corresponding node is founded for the ID " + ID );
		}
	}

	@Override
	public void list( final long ID )
	{
		// TODO implementare
		
	}
	
	/** Create the map associating each file to the corresponding node
	 * 
	 * @param files	list of files
	 * @param size	number of files
	 * 
	 * @return map of (ID, [fil1, file2, ...]) objects
	*/
	private HashMap<Long, List<String>> getNodeMap( final List<String> files, final int size )
	{
		HashMap<Long, List<String>> map = new HashMap<>( size );
		for(int i = 0; i < size; i++)
		{
			String file = files.get( i );
			long ID = ChordUtils.getID( file );
			
			List<String> list = map.get( ID );
			if(list == null)
				list = new ArrayList<>();
			list.add( file );

			map.put( ID, list );
		}
		
		return map;
	}
	
	/** Create the remote RMI Chord peer
	 * 
	 * @param address	remote IP address
	 * @param port		remote TCP port
	 * @param RMIname	RMI name registry
	*/
	private IChordRMIPeer createChordPeer( final String address, final short port, final String RMIname )
			throws MalformedURLException, RemoteException, NotBoundException
	{
		String URL = "rmi://" + address + ":" + port + "/" + RMIname;
		return (IChordRMIPeer) Naming.lookup( URL );
	}
	
	/** Interrupt the Chord system */
	public void stop()
	{
		// TODO implementare
		
	}
}