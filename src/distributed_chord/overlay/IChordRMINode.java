
package distributed_chord.overlay;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.json.JSONException;

public interface IChordRMINode extends Remote
{
	/** Return the node (as string) managing the input ID
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	 * @param ID		input ID
	*/
	public String findSuccessor( final long destID, final long ID ) throws RemoteException, JSONException, MalformedURLException, NotBoundException;
	
	/** Return the predecessor (as string) of the node
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	*/
	public String getSuccessor( final long destID ) throws RemoteException, JSONException;
	
	/** Return the predecessor (as string) of the node computing the search
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	 * @param ID		input ID
	*/
	public String findPredecessor( final long destID, final long ID ) throws RemoteException, JSONException, MalformedURLException, NotBoundException;
	
	/** Return the closest node preceding the input ID
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	 * @param ID		input ID
	*/
	public String closest_preceding_finger( final long destID, final long ID ) throws RemoteException, MalformedURLException, NotBoundException, JSONException;
	
	/** Return the predecessor (as string) of the node
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	*/
	public String getPredecessor( final long destID ) throws RemoteException, JSONException;

	/** Notify the successor node to check its predecessor
	 * 
	 * @param destID	destination node (used to forward to the right replica node)
	 * @param ID		own ID
	 * @param address	own IP address
	*/
	public void notifyPredecessor( final long destID, final long ID, final String address ) throws RemoteException, MalformedURLException, NotBoundException;
}