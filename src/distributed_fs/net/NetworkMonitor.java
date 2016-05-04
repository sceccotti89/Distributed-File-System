/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import distributed_fs.client.DFSService;
import distributed_fs.net.Networking.UDPnet;

/**
 * Class used to monitor the network state of the storage nodes.
*/
public abstract class NetworkMonitor extends Thread
{
	protected UDPnet net;
	protected final AtomicBoolean keepAlive = new AtomicBoolean( true );
	
	private static final String SEC_ALG = "AES"; /** Algorithm used to encrypt/decrypt the messages */
	private static final SecretKeySpec keySpec = new SecretKeySpec( DatatypeConverter.parseBase64Binary( "ABEiM0RVZneImaq7zN3u/w==" ), SEC_ALG );
    private static final IvParameterSpec ivSpec = new IvParameterSpec( DatatypeConverter.parseBase64Binary( "AAECAwQFBgcICQoLDA0ODw==" ) );
	
    protected static final Logger LOGGER = Logger.getLogger( DFSService.class );
    
	public NetworkMonitor( final String address ) throws IOException
	{
		//TODO LOGGER.setLevel( Utils.logLevel );
		
		net = new UDPnet();
		net.joinMulticastGroup( InetAddress.getByName( address ) );
	}
	
	/**
	 * Gets the statistics for a given node.
	 * 
	 * @param address
	*/
	public abstract NodeStatistics getStatisticsFor( final String address );
	
	/**
	 * Encrypts the incoming message.
	 * 
	 * @param message
	*/
	protected byte[] encryptMessage( final byte[] message ) throws Exception
	{
		final Cipher cipher = Cipher.getInstance( "AES/CBC/PKCS5Padding" );
		cipher.init( Cipher.ENCRYPT_MODE, keySpec, ivSpec );
		return cipher.doFinal( message );
	}
	
	/**
	 * Decrypts the incoming message.
	 * 
	 * @param message
	*/
	protected byte[] decryptMessage( final byte[] message ) throws Exception
	{
		final Cipher cipher = Cipher.getInstance( "AES/CBC/PKCS5Padding" );
		cipher.init( Cipher.DECRYPT_MODE, keySpec, ivSpec );
		return cipher.doFinal( message );
	}
	
	/**
	 * Closes the thread.
	*/
	public void shutDown()
	{
		net.close();
		keepAlive.set( false );
	}
}