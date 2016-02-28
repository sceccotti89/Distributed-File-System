
package distributed_chord.overlay;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IChordRMIPeer extends Remote
{
	/** Retrieve an object from this node
	 * 
	 * @param ID		node identifier
	 * @param files		list of files to store
	 * 
	 * @return TRUE if the operation is successfully completed, FALSE otherwise
	*/
	public boolean retrieve( final long ID, final List<String> files ) throws RemoteException;
	
	/** Store an object on this node
	 * 
	 * @param files		list of files to store
	 * @param bytes		bytes of the file
	 * 
	 * @return TRUE if the operation is successfully completed, FALSE otherwise
	*/
	public boolean store( final List<String> files, final List<byte[]> bytes ) throws IOException;
	
	/** Remove an object on this node
	 * 
	 * @param files		list of files to remove
	 * 
	 * @return TRUE if the operation is successfully completed, FALSE otherwise
	*/
	public boolean remove( final List<String> files ) throws RemoteException;
}