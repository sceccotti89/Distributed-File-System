/**
 * @author Stefano Ceccotti
*/

package distributed_chord.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.overlay.RemoteChordNode;

public class ChordUtils
{
	/** Maximum value on the ring */
	private static long MAX_VALUE;
	/** The hash function used to create the digest */
	private static HashFunction _hash;
	
	/** Resource path location */
	public static final String RESOURCE_LOCATION = "./Resources/";
	
	/** JSON message fields */
	public static final String JSON_HOST = "host";
	public static final String JSON_ID = "id";
	public static final String JSON_BOOT_NODE = "Boot IP";
	
	/** Type of messages */
	public static final char CHORD_WRITE = 0;
	public static final char CHORD_READ = 1;
	public static final char CHORD_DELETE = 2;
	
	/** Logger used to print the application state */
	public static final Logger LOGGER = Logger.getLogger( ChordUtils.class );
	
	public ChordUtils( final long max_value, final HashFunction hash )
	{
		MAX_VALUE = max_value;
		_hash = hash;
	}
	
	/** Compute the ID of a string
	 * 
	 * @param name	string name
	*/
	public static long getID( final String name )
	{
		long ID = _hash.digest_hash( name ) % MAX_VALUE;
		if(ID < 0) ID = MAX_VALUE + ID;
		
		return ID;
	}
	
	/** Serialize an object
	 * 
	 * @param location	file path location
	 * 
	 * @return the byte serialization of the object
	*/
	public static byte[] serializeObject( final String location ) throws IOException
	{
		File myFile = new File( location );
		int length = (int) myFile.length();
		byte bytes[] = new byte[length];
		
		FileInputStream fis = new FileInputStream( myFile );
		BufferedInputStream bis = new BufferedInputStream( fis );
		bis.read( bytes, 0, bytes.length );
		
		bis.close();
		fis.close();
		
		return bytes;
	}
	
	/** Deserialize and save an object from the input bytes
	 * 
	 * @param location	file path location
	 * @param bytes		bytes of the serialized file
	*/
	public static void deserializeObject( final String location, final byte bytes[] ) throws IOException
	{
		FileOutputStream fos = new FileOutputStream( location );
		BufferedOutputStream bos = new BufferedOutputStream( fos );
		
		bos.write( bytes, 0, bytes.length );
		bos.flush();
		
		fos.close();
		bos.close();
	}
	
	/** Check if the ID is already present in an internal structure
	 * 
	 * @param ID			node identifier
	 * @param finger_table	the finger table
	 * @param successor		the successors list
	 * 
	 * @return the corresponding node if found, null otherwise
	*/
	public static RemoteChordNode findNode( final long ID, final RemoteChordNode finger_table[], final RemoteChordNode successor[] )
	{
		int length = finger_table.length;
		for(int i = 0; i < length; i++)
		{
			RemoteChordNode node = finger_table[i];
			if(node != null && node.getID() == ID)
				return node;
		}
		
		length = successor.length;
		for(int i = 0; i < length; i++)
		{
			RemoteChordNode node = successor[i];
			if(node != null && node.getID() == ID)
				return node;
		}
		
		return null;
	}
	
	/** Check if the ID is inside the boundaries
	 * 
	 * @param ID				ID to be checked
	 * @param from				from index
	 * @param inclusive_from	if the left index must be included
	 * @param to				to index
	 * @param inclusive_to		if the right index must be included
	 * 
	 * @return TRUE if the ID is inside the boundaries, FALSE otherwise
	*/
	public static boolean isInside( final long ID, long from, final boolean inclusive_from, long to, final boolean inclusive_to )
	{
		//System.out.println( "[IS_INSIDE 1]: " + ID + ", FROM: " + from + ", TO: " + to );
		
		if(!inclusive_from) from = (from + 1) % MAX_VALUE;
		if(!inclusive_to){
			to--;
			if(to < 0) to = MAX_VALUE - 1;
		}
		
		//System.out.println( "[IS_INSIDE 2]: " + ID + ", FROM: " + from + ", TO: " + to );
		
		if(from <= to)			
			return ID >= from && ID <= to;
		else			
			return ID >= from || ID <= to;
	}
	
	/** Parse a JSON file
	 * 
	 * @param path	file location
	*/
	public static JSONObject parseJSONFile( final String path ) throws IOException, JSONException
	{
		BufferedReader file = new BufferedReader( new FileReader( path ) );
		StringBuilder content = new StringBuilder( 512 );
		String line;
		while((line = file.readLine()) != null)
			content.append( line );
		file.close();
		
		JSONObject obj = new JSONObject( content.toString() );
		return obj;
	}
}