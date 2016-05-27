/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.IOException;
import java.util.List;

import distributed_fs.exception.DFSException;
import distributed_fs.storage.DistributedFile;

public interface IDFSService
{
	/** 
	 * Retrieves an object from the corresponding node of the network.
	 * 
	 * @param fileName		name of the file to store
	 * 
	 * @return the downloaded file, if present, {@code null} otherwise
	*/
	public DistributedFile get( String fileName ) throws DFSException;
	
	/** 
	 * Stores an object on the corresponding node of the network.
	 * 
	 * @param fileName		name of the file to store
	 * 
	 * @return {@code true} if the write has been completed successfully, {@code false} otherwise
	*/
	public boolean put( String fileName ) throws DFSException, IOException;
	
	/** 
	 * Deletes an object from the corresponding node of the network.
	 * 
	 * @param fileName		name of the file to delete
	 * 
	 * @return {@code true} if the operation has been completed successfully, {@code false} otherwise
	*/
	public boolean delete( String fileName ) throws DFSException, IOException;
	
	/**
	 * Returns a list with all the files present in the database.
	*/
	public List<DistributedFile> listFiles();
}