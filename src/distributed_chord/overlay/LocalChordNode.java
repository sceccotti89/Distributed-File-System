
package distributed_chord.overlay;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.manager.UpdateChordThread;
import distributed_chord.utils.ChordUtils;
import distributed_chord.utils.HashFunction;
import distributed_chord.manager.FileManagementThread;

public class LocalChordNode extends ChordNode implements IChordRMINode
{
	/** List of successor nodes */
	//protected List<RemoteChordNode> successors;
	protected RemoteChordNode successor[];
	/** The successors and predecessor node */
	private RemoteChordNode predecessor;
	/** The finger table of the node */
	protected RemoteChordNode finger_table[];
	/** Next node to fix */
	private short next = -1;
	/** The passive thread */
	protected FileManagementThread file_mgr_t;
	/** The active thread */
	private UpdateChordThread update_t;
	/** Remote representation of this node */
	private RemoteChordNode me;
	
	/** Map of replicated virtual nodes */
	protected static final NavigableMap<Long, LocalChordNode> virtualNodes = new TreeMap<Long, LocalChordNode>();
	
	/** Maximum value on the Chord ring */
	//TODO private static final long MAX_VALUE = Long.MAX_VALUE >> 1;
	private static final long MAX_VALUE = 8;
	/** Number of bits of the Chord ring */
	//private static final int M = System.getProperty( "java.vm.name" ).contains( "64-Bit" ) ? 62 : 30;
	protected static final short M = 3; // TODO solo per test, poi usare il rigo precedente
	/** Number of successors */
	// TODO tipicamente R = log( N ), ma sapere N e' molto difficile, quindi settare R a poche decine 
	private static final short R = 10;
	/** Client configuration location */
	private static final String CLIENT_CONFIG = "./Settings/client_config.json";
	
	/** Generated serial ID */
	private static final long serialVersionUID = -3504338508293814246L;
	
	/** Construct a local node
	 * 
	 * @param virtualMembersPerBucket	number of virtual nodes
	 * @param _hash						hash function
	 * @param inet						network interface: if null will be assigned using the internal procedure
	*/
	public LocalChordNode( final short virtualMembersPerBucket, final HashFunction _hash, final String inet/*, final GossipMember boot_node*/ )
			throws IOException, NotBoundException
	{
		super( inet );
		
		new ChordUtils( MAX_VALUE, _hash );
		ChordUtils.LOGGER.info( "MAX: " + MAX_VALUE );
		
		ID = ChordUtils.getID( 0 + address );
		ChordUtils.LOGGER.info( "ID: " + ID );
		
		finger_table = new RemoteChordNode[M];
		//successors = new LinkedList<>(); // TODO ConcurrentLinkedQueue per il sincronismo
		successor = new RemoteChordNode[R];
		
		me = new RemoteChordNode( this.ID, this.address, true );
		
		if(virtualMembersPerBucket > 1)
		{
			ChordUtils.LOGGER.info( "Creating the " + virtualMembersPerBucket + " virtual nodes..." );
			
			virtualNodes.put( this.ID, this );
			
			for(short i = 1; i < virtualMembersPerBucket; i++)
			{
				LocalChordNode node = new LocalChordNode( i, address );
				virtualNodes.put( node.ID, node );
			}
			
			Iterator<Long> keys = virtualNodes.keySet().iterator();
			while(keys.hasNext())
				virtualNodes.get( keys.next() ).buildLocalFingerTable( virtualNodes );
			
			ChordUtils.LOGGER.info( "Virtual nodes created." );
		}
		
		update_t = new UpdateChordThread( virtualNodes );
		file_mgr_t = new FileManagementThread( this, MAX_VALUE );
	}
	
	/** Construct a local virtual node
	 * 
	 * @param vNodeID	virtual node identifier
	 * @param address	own IP address
	*/
	private LocalChordNode( final short vNodeID, final String address ) throws RemoteException, MalformedURLException, NotBoundException
	{
		super();
		
		this.address = address;
		finger_table = new RemoteChordNode[M];
		//successors = new LinkedList<>(); // TODO ConcurrentLinkedQueue per il sincronismo
		successor = new RemoteChordNode[R];
		
		ID = ChordUtils.getID( vNodeID + address );
		ChordUtils.LOGGER.info( "ID: " + ID );
		
		me = new RemoteChordNode( this.ID, this.address, true );
	}
	
	/** Build the local finger table of a virtual node
	 * 
	 * @param virtualNodes	virtual nodes map
	*/
	private void buildLocalFingerTable( final NavigableMap<Long, LocalChordNode> virtualNodes )
			throws RemoteException, MalformedURLException, NotBoundException
	{
		//System.out.println( "DENTRO: " + ID );
		
		// first the predecessor
		long pred;
		SortedMap<Long, LocalChordNode> map = virtualNodes.headMap( ID );
		if(map.size() == 0) pred = virtualNodes.lastKey();
		else pred = map.lastKey();
		predecessor = new RemoteChordNode( pred, this.address, true );
		//System.out.println( "PRED: " + pred );
		
		// then the finger table
		for(short i = 0; i < M; i++)
		{
			long ID = (this.ID + (1 << i)) % MAX_VALUE;
			// get the values greater or equals than the ID
			NavigableMap<Long, LocalChordNode> tail = virtualNodes.tailMap( ID, true );
			
			long lnode;
			// get the smallest among them
			if(tail.size() == 0) lnode = virtualNodes.firstKey();
			else lnode = tail.firstKey();
			//System.out.println( "FINGER[" + i + " : " + ID + "] = " + lnode );
			
			RemoteChordNode rnode = new RemoteChordNode( lnode, this.address, true );
			finger_table[i] = rnode;
		}
		
		// set the list of successors
		successor[0] = finger_table[0];
		Iterator<Long> keys = virtualNodes.keySet().iterator();
		short i = 1;
		while(i < R && keys.hasNext())
		{
			LocalChordNode node = virtualNodes.get( keys.next() );
			if(node.ID == successor[0].ID)
				break;
			
			successor[i++] = new RemoteChordNode( node.ID, this.address, true );
		}
	}
	
	/** Join the Network */
	public void join() throws IOException, JSONException, NotBoundException
	{
		RemoteChordNode boot_node = getBootstrapNode();
		
		file_mgr_t.start();
		
		if(virtualNodes.size() > 0)
		{
			if(boot_node != null)
			{
				Iterator<Long> keys = virtualNodes.keySet().iterator();
				while(keys.hasNext())
					virtualNodes.get( keys.next() ).join( boot_node );
			}
		}
		else
		{
			if(boot_node != null)
			{
				JSONObject j_node = new JSONObject( boot_node.getNode().findSuccessor( boot_node.ID, ID ) );
				finger_table[0] = predecessor = successor[0] = new RemoteChordNode( j_node, false );
				//successors.add( finger_table[0] );
			}
			else
			{
				// this is the only node in the network
				//RemoteChordNode node = new RemoteChordNode( this.ID, this.address, true );
				RemoteChordNode node = me;
				for(short i = 0; i < M; i++)
					finger_table[i] = node;
				//predecessor = successor[0] = null;// TODO mettere uguale a se' stesso?
			}
		}
		
		update_t.start();
	}

	/** Join the Network specifying the bootstrap node
	 * 
	 * @param boot_node	the bootstrap node
	*/
	private void join( final RemoteChordNode boot_node ) throws RemoteException, MalformedURLException, JSONException, NotBoundException
	{		
		JSONObject j_node = new JSONObject( boot_node.getNode().findSuccessor( boot_node.ID, ID ) );
		finger_table[0] = predecessor = successor[0] = new RemoteChordNode( j_node, false );
		//successors.add( finger_table[0] );
	}

	/** Contact the bootstrap node of the Chord network.
	 *  If something wrong it returns null.
	*/
	private RemoteChordNode getBootstrapNode() throws IOException, JSONException
	{
		// TODO questa parte va lasciata al gossiping?? meglio parlarne col prof anche se penso di si...
		
		RemoteChordNode boot_node = null;
		
		JSONObject obj = ChordUtils.parseJSONFile( CLIENT_CONFIG );
		JSONArray address_list = obj.getJSONArray( ChordUtils.JSON_BOOT_NODE );
		int length = address_list.length();
		for(int i = 0; i < length; i++)
		{
			String IPaddress = address_list.getString( i );
			if(!IPaddress.equals( address ))
			{
				ChordUtils.LOGGER.info( "Contacting: " + IPaddress + "..." );
				
				try
				{
					boot_node = new RemoteChordNode( IPaddress );
					ChordUtils.LOGGER.info( "Node: " + IPaddress + " found!" );
					break;
				}
				catch( RemoteException | NotBoundException | MalformedURLException e )
				{
					ChordUtils.LOGGER.info( "Node: " + IPaddress + " not found" );
					//e.printStackTrace();
				}
			}
		}
		
		return boot_node;
	}
	
	@Override
	public String closest_preceding_finger( final long destID, final long ID )
			throws RemoteException, MalformedURLException, NotBoundException, JSONException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			return node.closest_preceding_finger( node.ID, ID );
		}
		
		RemoteChordNode n = null;
		for(short i = M - 1; i >= 0; i--)
		{
			n = finger_table[i];
			if(n != null && ChordUtils.isInside( n.ID, this.ID, false, ID, false ))
				break;
		}
		
		long predID = (n == null) ? this.ID : n.ID;
		
		// check the list of successors
		for(short i = R - 1; i >= 0; i--)
		{
			RemoteChordNode node = successor[i];
			if(node != null && ChordUtils.isInside( node.ID, predID, false, ID, false ))
				return node.toJSONObject().toString();
		}
		
		if(n != null)
			return n.toJSONObject().toString();
		else
			return this.toJSONObject().toString();
	}
	
	/** During the stabilize procedure the node performs: <br>
	 * 
	 * 1) checks the availability of the predecessor node<br>
	 * 2) updates the successors list<br>
	 * 3) verifies the immediate predecessor of the first successor
	*/
	public void stabilize() throws RemoteException, JSONException, MalformedURLException, NotBoundException
	{
		RemoteChordNode node;
		
		if((node = predecessor) != null)
		{
			try{
				// random method used to check the availability of the predecessor node
				if(virtualNodes.containsKey( node.ID ))
					virtualNodes.get( node.ID ).getPredecessor( node.ID );
				else
					node.getNode().getPredecessor( node.ID );
			}
			catch( RemoteException e ){
				System.out.println( "[STABILIZE] Predecessor is no more reachable" );
				predecessor = null;
			}
		}
		
		updateSuccessorsList();
		
		if((node = successor[0]) != null && !node.address.equals( this.address ))
		{
			IChordRMINode i_succ = node.getNode();
			long succID = node.ID;
			//System.out.println( "[STABILIZE] SUCCESSOR: " + node.toJSONObject().toString() );
			
			try
			{
				String pred;
				if(virtualNodes.containsKey( succID ))
					pred = virtualNodes.get( succID ).getPredecessor( succID );
				else
					pred = i_succ.getPredecessor( succID );
				//System.out.println( "[STABILIZE] PRED: " + pred );
				
				if(pred != null)
				{
					JSONObject response = new JSONObject( pred );
					long hostID = response.getLong( ChordUtils.JSON_ID );
					//System.out.println( "[STABILIZE] CHECK: " + ChordUtils.isInside( hostID, this.ID, false, successor[0].ID, false ) );
					if(ChordUtils.isInside( hostID, this.ID, false, succID, false )) // check if the predecessor between itself and the node
					{
						String hostAddress = response.getString( ChordUtils.JSON_HOST );
						//successors.set( 0, finger_table[0] = successor[0] = new RemoteChordNode( hostID, response.getString( JSON_HOST ) ) );
						if((node = ChordUtils.findNode( hostID, finger_table, successor )) == null)
							node = new RemoteChordNode( hostID, hostAddress, virtualNodes.containsKey( hostID ) );
						
						finger_table[0] = successor[0] = node;
						i_succ = node.getNode();
						succID = node.ID;
						//successor.getNode().notifyPredecessor( succID, this.ID, this.address );
					}
				}
				//else
				if(virtualNodes.containsKey( succID ))
					virtualNodes.get( succID ).notifyPredecessor( succID, this.ID, this.address );
				else
					i_succ.notifyPredecessor( succID, this.ID, this.address );
			}
			catch( RemoteException e ){}
		}
	}
	
	/** Update the list of successor nodes */
	private void updateSuccessorsList() throws RemoteException, MalformedURLException, NotBoundException
	{
		/*if(successors.size() > 0)
		{
			boolean founded = false;
			int length = successor.length;
			RemoteChordNode node = successors.get( 0 );
			short i = 1;
			while(i < R)
			{
				try{
					String succ = node.getNode().getSuccessor( node.ID );
					if(succ != null)
					{
						JSONObject j_node = new JSONObject( succ );
						long id = j_node.getLong( JSON_ID );
						if(id == this.ID) // end of the cycle
							break;
						
						founded = true;
						
						if(i >= length || id != successors.get( i ).ID)
							successors.add( node = new RemoteChordNode( j_node ) );
						
						i++;
					}
				}
				catch( Exception e ){}
				
				if(!founded)
				{
					// the successor is no more reachable
					successors.remove( Math.min( i, length ) );
					length--;
				}
			}
		}*/
		
		//RemoteChordNode node = new RemoteChordNode( this.ID, this.address, true );
		RemoteChordNode node = me;
		short i = 0, length = R;
		boolean founded;
		
		while(i < length)
		{
			founded = false;
			
			try{
				String succ;
				if(virtualNodes.containsKey( node.ID ))
					succ = virtualNodes.get( node.ID ).getSuccessor( node.ID );
				else
					succ = node.getNode().getSuccessor( node.ID );
				if(succ != null)
				{
					JSONObject j_node = new JSONObject( succ );
					long id = j_node.getLong( ChordUtils.JSON_ID );
					if(id == this.ID) // end of the cycle
						break;
					
					RemoteChordNode r_succ;
					if((r_succ = successor[i]) == null || id != r_succ.ID)
					{
						String hostAddress = j_node.getString( ChordUtils.JSON_HOST );
						if((r_succ = ChordUtils.findNode( id, finger_table, successor )) == null)
							r_succ = new RemoteChordNode( id, hostAddress, virtualNodes.containsKey( j_node.getLong( ChordUtils.JSON_ID ) ) );
						
						successor[i] = r_succ;
					}
					
					node = successor[i++];
					founded = true;
				}
			}
			catch( Exception e ){}
			
			if(!founded)
			{
				// the i-th successor is no more reachable: copy the next successor in the previous position
				for(short j = i; j < R - 1; j++)
					successor[j] = successor[j + 1];
				successor[--length] = null;
				
				if(successor[i] == null)
					break;
				else
					node = successor[i];
			}
		}
		
		// set NULL all the other successors
		for( ; i < R; i++)
			successor[i] = null;
		
		// print the successors list
		for(i = 0; i < R; i++)
		{
			if(successor[i] != null)
				System.out.println( "SUCCESSOR[" + i + "] = " + successor[i].ID );
		}
	}
	
	@Override
	public void notifyPredecessor( final long destID, final long ID, final String address )
			throws RemoteException, MalformedURLException, NotBoundException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			node.notifyPredecessor( node.ID, ID, address );
			return;
		}
		
		//if(predecessor != null)
			//System.out.println( "NOTIFY: NO_NULL" + ", CHECK: " + ChordUtils.isInside( ID, predecessor.ID, false, this.ID, false ) );
		//else
			//System.out.println( "NOTIFY: NULL" );
		
		//System.out.println( "NOTIFY: " + ID + ", ADDRESS: " + address );
		
		RemoteChordNode node;
		if((node = predecessor) == null || ChordUtils.isInside( ID, node.ID, false, this.ID, false ))
		{
			file_mgr_t.sendFiles( ID, address );
			
			if((node = ChordUtils.findNode( ID, finger_table, successor )) == null)
				node = new RemoteChordNode( ID, address, virtualNodes.containsKey( ID ) );
			predecessor = node;
			
			if((node = successor[0]) == null || node.ID == this.ID)
				finger_table[0] = successor[0] = predecessor;
			
			//System.out.println( "[NOTIFY]: AGGIUNTO: " + predecessor.address + ", SUCC: " + successor[0].address );
		}
	}
	
	/** Update the next finger */
	public void fix_fingers() throws IOException, JSONException, NotBoundException
	{
		next = (short) ((next + 1) % M);
		long ID = (this.ID + (1 << next)) % MAX_VALUE;
		System.out.println( "[FIX_FINGER]: NEXT = " + next );
		
		// check if the owner is itself
		RemoteChordNode node;
		if((node = predecessor) != null && ChordUtils.isInside( ID, node.ID, false, this.ID, true ))
		{
			//node = new RemoteChordNode( this.ID, this.address, true );
			node = me;
			finger_table[next] = node;
			if(next == 0)
				successor[0] = node;
			return;
		}
		
		try
		{
			String response = findSuccessor( this.ID, ID );
			if(response != null)
			{
				JSONObject j_node = new JSONObject( response );
				
				long id = j_node.getLong( ChordUtils.JSON_ID );
				if((node = ChordUtils.findNode( id, finger_table, successor )) == null)
					node = new RemoteChordNode( j_node, virtualNodes.containsKey( id ) );
				
				finger_table[next] = node;
				if(next == 0)
					successor[0] = node;
			}
		}
		catch( RemoteException e )
		{
			// the nodes with the same ID are no more reachable
			//node = new RemoteChordNode( this.ID, this.address, true );
			node = me;
			for(short j = 0; j < M; j++)
			{
				if((node = finger_table[j]) != null && node.ID == ID)
					finger_table[j] = node;
			}
			
			if(next == 0)
				successor[0] = null;
		}
	}

	@Override
	public String findSuccessor( final long destID, final long ID ) throws RemoteException, JSONException, MalformedURLException, NotBoundException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			return node.findSuccessor( node.ID, ID );
		}
		
		//System.out.println( "[FIND_SUCC] PREDECESSOR: " + predecessor );
		
		//if(predecessor != null)
			//System.out.println( "[FIND_SUCC] ID: " + ID + ", PRED: " + predecessor.ID +
			//", INSIDE: " + ChordUtils.isInside( ID, predecessor.ID, false, this.ID, true ) );
		
		String pred = findPredecessor( this.ID, ID );
		if(pred == null)
			return toJSONObject().toString();
		
		RemoteChordNode node;
		
		JSONObject j_node = new JSONObject( pred );
		long id = j_node.getLong( ChordUtils.JSON_ID );
		if((node = ChordUtils.findNode( id, finger_table, successor )) == null)
			node = new RemoteChordNode( j_node, virtualNodes.containsKey( id ) );
		
		if(virtualNodes.containsKey( id ))
			return virtualNodes.get( id ).getSuccessor( id );
		else
			return node.getNode().getSuccessor( id );
	}
	
	@Override
	public String getSuccessor( final long destID ) throws RemoteException, JSONException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			return node.getSuccessor( node.ID );
		}
		
		RemoteChordNode node;
		if((node = successor[0]) == null)
			return null;
		else
			return node.toJSONObject().toString();
	}
	
	@Override
	public String findPredecessor( final long destID, final long ID )
			throws RemoteException, JSONException, MalformedURLException, NotBoundException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			return node.findPredecessor( node.ID, ID );
		}
		
		//System.out.println( "[FIND_PRED] SUCCESSOR: " + successor[0] );
		RemoteChordNode succ;
		if((succ = successor[0]) == null)
			return null;
		
		//RemoteChordNode node = new RemoteChordNode( this.ID, this.address, true );
		RemoteChordNode node = me;
		while(true)
		{
			if(ChordUtils.isInside( ID, node.ID, false, succ.ID, true ))
				break;
			
			JSONObject j_node;
			if(virtualNodes.containsKey( node.ID ))
				j_node = new JSONObject( virtualNodes.get( node.ID ).closest_preceding_finger( node.ID, ID ) );
			else
				j_node = new JSONObject( node.getNode().closest_preceding_finger( node.ID, ID ) );
			long id = j_node.getLong( ChordUtils.JSON_ID );
			if((node = ChordUtils.findNode( id, finger_table, successor )) == null)
				node = new RemoteChordNode( j_node, virtualNodes.containsKey( id ) );
			
			String succ_node;
			if(virtualNodes.containsKey( node.ID ))
				succ_node = virtualNodes.get( node.ID ).getSuccessor( node.ID );
			else
				succ_node = node.getNode().getSuccessor( node.ID );
			if(succ_node == null)
				break;
			
			JSONObject j_succ = new JSONObject( succ_node );
			id = j_succ.getLong( ChordUtils.JSON_ID );
			if((succ = ChordUtils.findNode( id, finger_table, successor )) == null)
				succ = new RemoteChordNode( j_succ, virtualNodes.containsKey( id ) );
			//succ = new RemoteChordNode( j_succ, virtualNodes.containsKey( j_succ.getLong( ChordUtils.JSON_ID ) ) );
			//System.out.println( "[FIND_PRED] PARZIALE: " + succ.address + ":" + succ.ID );
		}
		
		System.out.println( "[FIND_PRED] TROVATO: " + node.address );
		
		return node.toJSONObject().toString();
	}

	@Override
	public String getPredecessor( final long destID ) throws RemoteException, JSONException
	{
		if(destID != this.ID)
		{
			LocalChordNode node = virtualNodes.get( destID );
			return node.getPredecessor( node.ID );
		}
		
		RemoteChordNode node;
		if((node = predecessor) == null)
			return null;
		else
			return node.toJSONObject().toString();
	}

	/** Print the finger table */
	public void printFingerTable() throws JSONException
	{
		for(short i = 0; i < M; i++)
		{
			RemoteChordNode node = finger_table[i];
			//ChordService.LOGGER.info( "FINGER[" + i + "] = " + ((node == null) ? "null" : node.toJSONObject()) );
			System.out.println( "FINGER[" + i + "] = " + ((node == null) ? "null" : node.toJSONObject()) );
		}
	}
	
	/** Check if a key is contained in the set of virtual nodes
	 * 
	 * @param key	input key
	 * 
	 * @return TRUE if the key is contained, FALSE otherwise
	*/
	public boolean containsKey( final long key )
	{
		return virtualNodes.containsKey( key );
	}
	
	/** Return the list of successors */
	public RemoteChordNode[] getSuccessors()
	{
		return successor;
	}
	
	/** Return the finger table of the node */
	public RemoteChordNode[] getFingerTable()
	{
		return finger_table;
	}
}