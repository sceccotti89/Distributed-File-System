/**
 * @author Stefano Ceccotti
*/

package distributed_chord;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.security.NoSuchAlgorithmException;

import org.json.JSONException;

import distributed_chord.manager.ChordManager;

public class Main
{
	public static void main( String args[] ) throws NoSuchAlgorithmException, IOException, JSONException, NotBoundException
	{
		/*HashFunction _hash = new HashFunction( HashFunction._HASH.SHA_256 );
		
		String inet = null;
		
		int length = args.length;
		if(length > 0)
			inet = args[0];
		
		ChordPeer node = new ChordPeer( (short) 3, _hash, inet );
		node.join();*/
		
		ChordManager manager = new ChordManager( args );
		manager.start();
		
		//manager.put( Arrays.asList( "" ) );
		
		//manager.stop();
	}
}