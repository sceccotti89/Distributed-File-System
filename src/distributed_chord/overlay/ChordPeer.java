
package distributed_chord.overlay;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

import org.json.JSONException;

import distributed_chord.utils.ChordUtils;
import distributed_chord.utils.HashFunction;

public class ChordPeer implements /*IChordPeer, */IChordRMIPeer
{
	/** Associated local node */
	private LocalChordNode _node;
	
	/** Port used for the RMI communications */
	private static final short PORT = 7400;
	/** RMI name address */
	private static final String RMIname = "ChordPeer";
	/** Parameters of the quorum protocol (like Dynamo) */
	private static final short T = 3, W = 2, R = 2;
	
	public ChordPeer( final short virtualMembersPerBucket, final HashFunction hash, final String inet ) throws IOException, NotBoundException
	{
		_node = new LocalChordNode( virtualMembersPerBucket, hash, inet );
		
		// create the RMI registry
		System.setProperty( "java.rmi.server.hostname", _node.address );
		IChordRMIPeer objServer = this;
		Registry reg = LocateRegistry.createRegistry( PORT );		
		reg.rebind( RMIname, objServer );
	}
	
	/** Join the Chord network */
	public void join() throws IOException, JSONException, NotBoundException
	{
		_node.join();
	}
	
	/** Return the associated local node */
	public LocalChordNode getNode()
	{
		return _node;
	}
	
	/** Return the RMI port */
	public short getPort()
	{
		return PORT;
	}
	
	/** Return the RMI name used */
	public String getRMIName()
	{
		return RMIname;
	}
	
	/*@Override
	public void get( final List<String> files ) throws JSONException, NotBoundException, IOException
	{
		int size = files.size();
		HashMap<Long, List<String>> map = getNodeMap( files, size );
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.ID, ID );
			if(s_node != null)
			{
				List<String> data = map.get( ID );
				
				JSONObject j_node = new JSONObject( s_node );
				long hostID = j_node.getLong( ChordUtils.JSON_ID );
				
				if(LocalChordNode.virtualNodes.containsKey( hostID ))
				{
					// if the node is local, just compute the retrieve operation
					if(!retrieve( ID, data ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.finger_table, _node.successor );
	
				try{
					IChordRMIPeer peer = node.getPeer();
					if(peer == null)
					{
						String URL = "rmi://" + node.address + ":" + PORT + "/" + RMIname;
						peer = (IChordRMIPeer) Naming.lookup( URL );
						node.setPeer( peer );
					}
					
					if(!peer.retrieve( ID, data ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.address + " not reachable" );
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
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.ID, ID );
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
				
				if(LocalChordNode.virtualNodes.containsKey( hostID ))
				{
					// if the node is local, just compute the store operation
					if(!store( data, bytes ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.finger_table, _node.successor );
	
				try{
					IChordRMIPeer peer = node.getPeer();
					if(peer == null)
					{
						String URL = "rmi://" + node.address + ":" + PORT + "/" + RMIname;
						peer = (IChordRMIPeer) Naming.lookup( URL );
						node.setPeer( peer );
					}
					
					if(!peer.store( data, bytes ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.address + " not reachable" );
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
		
		for(long ID : map.keySet())
		{
			String s_node = _node.findSuccessor( _node.ID, ID );
			if(s_node != null)
			{
				List<String> data = map.get( ID );
				
				JSONObject j_node = new JSONObject( s_node );
				long hostID = j_node.getLong( ChordUtils.JSON_ID );
				
				if(LocalChordNode.virtualNodes.containsKey( hostID ))
				{
					// if the node is local, just compute the delete operation
					if(!remove( data ))
						System.out.println( "Operation not performed: try again later" );
					
					return;
				}
				
				RemoteChordNode node = ChordUtils.findNode( hostID, _node.finger_table, _node.successor );
	
				try{
					IChordRMIPeer peer = node.getPeer();
					if(peer == null)
					{
						String URL = "rmi://" + node.address + ":" + PORT + "/" + RMIname;
						peer = (IChordRMIPeer) Naming.lookup( URL );
						node.setPeer( peer );
					}
					
					if(!peer.remove( data ))
						System.out.println( "Operation not performed: try again later" );
				}
				catch( MalformedURLException | RemoteException | NotBoundException e )
				{
					System.out.println( "Node " + _node.address + " not reachable" );
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
		
	}*/
	
	/** Create the map associating each file to the corresponding node
	 * 
	 * @param files	list of files
	 * @param size	number of files
	 * 
	 * @return map of (ID, [fil1, file2, ...]) objects
	*/
	/*private HashMap<Long, List<String>> getNodeMap( final List<String> files, final int size )
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
	}*/
	
	// ************************** RMI METHODS ************************** //

	@Override
	public boolean retrieve( final long ID, final List<String> files ) throws RemoteException
	{
		short response = 0;
		for(short i = 0; i < T; i++)
		{
			RemoteChordNode node = _node.successor[i];
			if(node != null)
			{
				// TODO testare se la lettura e' andata a buon fine, solo allora incrementa il responso
				
				response++;
			}
		}
		
		return response >= R;
	}
	
	@Override
	public boolean store( final List<String> files, final List<byte[]> bytes ) throws IOException
	{
		// save the received file
		
		// TODO salvare solo se si raggiunge il quorum o in ogni caso?
		int size = files.size();
		for(int i = 0; i < size; i++)
			ChordUtils.deserializeObject( ChordUtils.RESOURCE_LOCATION, bytes.get( i ) );
		
		short response = 0;
		for(short i = 0; i < T; i++)
		{
			RemoteChordNode node = _node.successor[i];
			if(node != null)
			{
				if(LocalChordNode.virtualNodes.containsKey( node.ID )) // if the node is local, just increase the counter
					response++;
				else{
					if(_node.file_mgr_t.sendFiles( files, bytes, node.address ))
						response++;
					
					// TODO e' efficiente questa soluzione (usare cioe' RMI)? forse mi conviene usare il TCP del PassiveChordThread
					/*IChordRMIPeer peer = node.getPeer();
					
					try{
						if(peer == null)
						{
							String URL = "rmi://" + node.address + ":" + PORT + "/" + RMIname;
							peer = (IChordRMIPeer) Naming.lookup( URL );
							node.setPeer( peer );
						}
						
						if(peer.store( file, bytes, false ))
							response++;
					}
					catch( MalformedURLException | RemoteException | NotBoundException e )
					{
						System.out.println( "Node " + node.address + " not reachable" );
					}*/
				}
			}
		}
		
		return response >= W;
	}
	
	@Override
	public boolean remove( final List<String> files ) throws RemoteException
	{
		short response = 0;
		for(short i = 0; i < T; i++)
		{
			RemoteChordNode node = _node.successor[i];
			if(node != null)
			{
				if(_node.file_mgr_t.sendDeleteFiles( files, node.address ))
					response++;
			}
		}
		
		//return response >= M;
		
		// TODO anche qui capire se vanno eliminati in ogni caso o solo se c'e' il quorum (come adesso)?
		if(response >= LocalChordNode.M)
		{
			int size = files.size();
			for(int i = 0; i < size; i++)
				_node.file_mgr_t.removeFile( files.get( i ) );
			
			return true;
		}
		else
			return false;
	}
}