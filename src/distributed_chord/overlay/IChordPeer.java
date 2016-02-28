
package distributed_chord.overlay;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.List;

import org.json.JSONException;

public interface IChordPeer
{	
	/** Retrieve one or more objects from the corresponding ID on the network
	 * 
	 * @param files	list of files to store
	*/
	public void get( final List<String> files ) throws JSONException, NotBoundException, IOException;
	
	/** Store one or more objects on the corresponding ID on the network
	 * 
	 * @param files	list of files to store
	 * 
	 * @return TRUE if the write is completed with successfull, FALSE otherwise
	*/
	public void put( final List<String> files ) throws JSONException, IOException, NotBoundException;
	
	/** Delete one or more object from the corresponding ID on the network
	 * 
	 * @param files	list of files to delete
	*/
	public void delete( final List<String> files ) throws JSONException, NotBoundException, IOException;
	
	/** List all the files contained in the specified node
	 * 
	 * @param ID	the specified host
	*/
	public void list( final long ID );
}