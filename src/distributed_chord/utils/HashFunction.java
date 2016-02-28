
package distributed_chord.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunction
{
	/** Create a digest of the input */
	private MessageDigest digest;
	
	/** Hash function used in the digest procedure */
	public static enum _HASH
	{
		SHA_1, SHA_256, SHA_512,
		MD5
	};

	public HashFunction( final _HASH type ) throws NoSuchAlgorithmException
	{
		String _hash = type.toString();
		int index = _hash.indexOf( '_' );
		if(index >= 0)
			_hash = _hash.substring( 0, index ) + "-" + _hash.substring( index + 1 );
		
		digest = MessageDigest.getInstance( _hash );
	}
	
	/** Return the long representation of the hash function's digest
	 * 
	 * @param key	the input key
	*/
	public long digest_hash( final String key )
	{		
		byte[] h_bytes = digest.digest( key.getBytes() );
		ByteBuffer wrapper = ByteBuffer.wrap( h_bytes );
		
		return wrapper.getLong();
	}
}