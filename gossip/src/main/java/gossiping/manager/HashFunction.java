
package gossiping.manager;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunction
{
	/** Create a digest of the input */
	private MessageDigest digest;
	
	/** Hash functions provided */
	public static enum _HASH
	{
		SHA_1, SHA_256, SHA_512,
		MD5
	};

	public HashFunction( final _HASH type )
	{
		String _hash = type.toString();
		int index = _hash.indexOf( '_' );
		if(index >= 0)
			_hash = _hash.substring( 0, index ) + "-" + _hash.substring( index + 1 );
		
		try { digest = MessageDigest.getInstance( _hash ); }
		catch( NoSuchAlgorithmException e ) {}
	}
	
	/**
     * Returns the byte digest representation of the given key.
     * 
     * @param key   the given key
    */
    public byte[] hashBytes( final String key )
    {
        return hashBytes( key.getBytes( StandardCharsets.UTF_8 ) );
    }
	
	/**
	 * Returns the byte digest representation of the given key.
	 * 
	 * @param key	the given key
	*/
	public byte[] hashBytes( final byte[] key )
	{
		return digest.digest( key );
	}
	
	/**
     * Returns the long digest representation of the given key.
     * 
     * @param key   the given key
    */
    public long hashLong( final String key )
    {
        return hashLong( key.getBytes( StandardCharsets.UTF_8 ) );
    }
	
	/**
	 * Returns the long digest representation of the given key.
	 * 
	 * @param key	the input key
	*/
	public long hashLong( final byte[] key )
	{		
		byte[] h_bytes = digest.digest( key );
		ByteBuffer wrapper = ByteBuffer.wrap( h_bytes );
		
		return wrapper.getLong();
	}
}