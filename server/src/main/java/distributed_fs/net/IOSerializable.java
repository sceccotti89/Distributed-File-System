/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

/**
 * Interface used to serialize/deserialize an object
 * in a more efficient manner compared to the default
 * {@link java.io.Serializable} interface.<br>
 * The time and space improvements can be obtained considering that
 * this interface doesn't waste time to serialize/deserialize
 * all the methods and the serial id, present in the class.
*/
public interface IOSerializable
{
	/**
	 * Transforms the object in a byte array.
	*/
	public byte[] read();
	
	/**
	 * Reads the object from a byte array.
	 * 
	 * @param data	object from which the informations are taken
	*/
	public void write( final byte[] data );
}