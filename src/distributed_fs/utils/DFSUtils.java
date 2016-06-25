/**
 * @author Stefano Ceccotti
*/

package distributed_fs.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Level;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.HashFunction;

import distributed_fs.overlay.DFSNode;
import gossiping.StartupSettings;

public class DFSUtils
{
	/** Hash function used to compute the identifier of a node. */
	public static final HashFunction _hash = StartupSettings._hash;
	
	/** Configuration path. */
	public static final String DISTRIBUTED_FS_CONFIG = "./Settings/Settings.json";
	
	/** The logger level. */
	public static Level logLevel;
	
	/** Port used to send/receive data in the service. */
	public static final int SERVICE_PORT = 9000;
	
	/** Decides whether the program is running in test mode or not. */
	public static boolean testing = false;
	
	/** Decides whether the configurations are initialized. */
	public static boolean initConfig = false;
	
	/**
	 * Returns the identifier associated to the node.
	 * 
	 * @param virtualNode	the virtual node instance
	 * @param address		the network address of the object
	*/
	public static String getNodeId( final int virtualNode, final String address )
	{
		byte[] hostInBytes = address.getBytes( StandardCharsets.UTF_8 );
		ByteBuffer bb = ByteBuffer.allocate( Integer.BYTES + hostInBytes.length );
		bb.putInt( virtualNode );
		bb.put( hostInBytes );
		
		return bytesToHex( _hash.hashBytes( bb.array() ).asBytes() );
	}
	
	/**
	 * Returns the identifier associated to the given object.
	 * 
	 * @param object	the given object. It, and all of its superclasses,
	 * 					must implement the {@link java.io.Serializable} interface
	*/
	public static <S extends Serializable> String getId( final S object )
	{
		byte[] bucketNameInBytes = serializeObject( object );
		return bytesToHex( _hash.hashBytes( bucketNameInBytes ).asBytes() );
	}
	
	/**
	 * Returns the number of virtual nodes that you can manage,
	 * based on the capabilities of this machine.
	*/
	public static short computeVirtualNodes() throws IOException
	{
		short virtualNodes = 2;
		
		Runtime runtime = Runtime.getRuntime();
		
		/* Total number of processors or cores available to the JVM */
		int cores = runtime.availableProcessors();
		DFSNode.LOGGER.debug( "Available processors: " + cores + ", CPU nodes: " + (cores * 4) );
		virtualNodes = (short) (virtualNodes + (cores * 4));
		
		/* Size of the RAM */
		long RAMsize;
		String OS = System.getProperty( "os.name" ).toLowerCase();
		if(OS.startsWith( "windows" )) { // windows command
			ProcessBuilder pb = new ProcessBuilder( "wmic", "computersystem", "get", "TotalPhysicalMemory" );
			Process proc = pb.start();
            //Process proc = runtime.exec( "wmic computersystem get TotalPhysicalMemory" );
			short count = 0;
			
			InputStream stream = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader( stream );
			BufferedReader br = new BufferedReader( isr );
			
			String line = null;
			while((line = br.readLine()) != null && ++count < 3);
			
			br.close();
			
			//System.out.println( line );
			RAMsize = Long.parseLong( line.trim() );
		}
		else { // linux command
			ProcessBuilder pb = new ProcessBuilder( "less", "/proc/meminfo" );
			Process proc = pb.start();
			
			InputStream stream = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader( stream );
			BufferedReader br = new BufferedReader( isr );
			
			String line = null;
			while((line = br.readLine()) != null) {
				if(line.startsWith( "MemTotal" ))
					break;
			}
			
			br.close();
			
			Matcher matcher = Pattern.compile( "[0-9]+(.*?)[0-9]" ).matcher( line );
			matcher.find();
			// Multiply it by 1024 because the result is in kB
			RAMsize = Long.parseLong( line.substring( matcher.start(), matcher.end() ) ) * 1024;
		}
		
		DFSNode.LOGGER.debug( "RAM size: " + RAMsize + ", RAM nodes: " + (RAMsize / 262144000) );
		virtualNodes = (short) (virtualNodes + (RAMsize / 262144000)); // divide it by 250MB
		
		DFSNode.LOGGER.debug( "Total nodes: " + virtualNodes );
		
		return virtualNodes;
	}
	
	/** 
	 * Serializes an object.
	 * 
	 * @param obj	the object to serialize. It must implements the
	 *				{@link java.io.Serializable} interface
	 * 
	 * @return the byte serialization of the object, if no error happens, null otherwise
	*/
	public static <T extends Serializable> byte[] serializeObject( final T obj )
	{
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream( out );
			os.writeObject( obj );
			os.close();
			
			return out.toByteArray();
		}
		catch( IOException e ){
			return null;
		}
	}
	
	/** 
	 * Deserializes an object from the given byte data.
	 * 
	 * @param data		bytes of the serialized object
	 * 
	 * @return the deserialization of the object,
	 * 		   casted to the type specified in {@link T}
	*/
	public static <T extends Serializable> T deserializeObject( final byte data[] )
	{
		try {
			ByteArrayInputStream in = new ByteArrayInputStream( data );
			ObjectInputStream is = new ObjectInputStream( in );
			
			@SuppressWarnings("unchecked")
			T obj = (T) is.readObject();
			is.close();
			
			return obj;
		}
		catch( ClassNotFoundException | IOException e ){
			return null;
		}
	}
	
	/**
	 * Creates a new directory, if it doesn't exist.
	 * 
	 * @param dirPath	path to the directory
	 * 
	 * @return {@code true} if the direcotry has been created,
	 * 		   {@code false} otherwise.
	*/
	public static boolean createDirectory( final String dirPath )
	{
		return createDirectory( new File( dirPath ) );
	}
	
	/**
	 * Creates a new directory, if it doesn't exist.
	 * 
	 * @param dirFile	the directory file
	 * 
	 * @return {@code true} if the direcotry has been created,
	 * 		   {@code false} otherwise.
	*/
	public static boolean createDirectory( final File dirFile )
	{
		if(dirFile.exists())
			return true;
		
		return dirFile.mkdirs();
	}
	
	/**
     * Checks whether a file exists.
     * 
     * @param file                  the file to check
     * @param createIfNotExists     setting it to {@code true} the file will be created if it shouldn't exists,
     *                              {@code false} otherwise
    */
    public static boolean existFile( final File file, final boolean createIfNotExists ) throws IOException
    {
        return existFile( file.getAbsolutePath(), createIfNotExists );
    }
	
	/**
	 * Checks whether a file exists.
	 * 
	 * @param filePath				path to the file to check
	 * @param createIfNotExists		setting it to {@code true} the file will be created if it shouldn't exists,
	 * 								{@code false} otherwise
	*/
	public static boolean existFile( final String filePath, final boolean createIfNotExists )
	        throws IOException
	{
		File file = new File( filePath );
		boolean exists = file.exists();
		if(!exists && createIfNotExists) {
		    if(file.getParent() != null)
		        file.getParentFile().mkdirs();
			file.createNewFile();
		}
		
		return exists;
	}
	
	/**
	 * Checks whether a file is a directory.
	 * 
	 * @param filePath		path to the file
	*/
	public static boolean isDirectory( final String filePath )
	{
		File file = new File( filePath );
		return file.exists() && file.isDirectory();
	}
	
	/**
	 * Generates a random file as string.
	*/
	public static String createRandomFile()
	{
		int size = (int) (Math.random() * 5) + 1; // Max 5 bytes
		StringBuilder builder = new StringBuilder( size );
		for(int i = 0; i < size; i++) {
			builder.append( (char) ((Math.random() * 57) + 65) );
		}
		
		return builder.toString();
	}
	
	/** 
	 * Reads and serializes a file from disk.
	 * 
	 * @param filePath		path to the file to read
	 * 
	 * @return the byte serialization of the object
	*/
	public static byte[] readFileFromDisk( final String filePath ) throws IOException
	{
		File file = new File( filePath );
		
		int length = (int) file.length();
		byte bytes[] = new byte[length];
		
		FileInputStream fis = new FileInputStream( file );
		BufferedInputStream bis = new BufferedInputStream( fis );
		bis.read( bytes, 0, bytes.length );
		
		bis.close();
		fis.close();
		
		return bytes;
	}

	/** 
	 * Saves a file on disk.
	 * 
	 * @param filePath	path where the file have to be write
	 * @param content	bytes of the serialized object
	*/
	public static void saveFileOnDisk( final String filePath, final byte content[] ) throws IOException
	{
		if(content == null) 
			DFSUtils.createDirectory( filePath );
		else {
			// Test whether the path to that file doesn't exist.
			// In that case create all the necessary directories
			File file = new File( filePath ).getParentFile();
			if(!file.exists())
				file.mkdirs();
			
			FileOutputStream fos = new FileOutputStream( filePath );
			BufferedOutputStream bos = new BufferedOutputStream( fos );
			
			bos.write( content, 0, content.length );
			bos.flush();
			
			fos.close();
			bos.close();
		}
	}
	
	/**
	 * Deletes all the content of a directory.<br>
	 * If it contains other folders inside, them will be deleted too
	 * in a recursive manner.
	 * 
	 * @param dir  the current directory
	*/
	public static void deleteDirectory( final File dir )
	{
	    if(dir.exists()) {
    	    for(File f : dir.listFiles()) {
    	        if(f.isDirectory())
    	            deleteDirectory( f );
    	        f.delete();
    	    }
    	    
    	    dir.delete();
	    }
	}
	
	/** 
	 * Deletes a file on disk.
	 * 
	 * @param file	the name of the file
	*/
	public static void deleteFileOnDisk( final String file )
	{
		File f = new File( file );
		if(f.exists())
			f.delete();
	}
	
	/**
	 * Compresses data using a GZIP compressor.
	 * 
	 * @param data	the bytes to compress
	 * 
	 * @return the compressed bytes array
	*/
	public static byte[] compressData( final byte[] data )
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try{
			GZIPOutputStream zos = new GZIPOutputStream( baos );
			zos.write( data );
			zos.close();
		}
		catch( IOException e ){ return null; }

		return baos.toByteArray();
	}
	
	/**
	 * Decompresses data using a GZIP decompressor.
	 * This method works only if the input data has been
	 * compressed with the {@link #compressData(byte[])} method.
	 * 
	 * @param data	the bytes to decompress
	 * 
	 * @return the decompressed bytes array
	*/
	public static byte[] decompressData( final byte[] data ) throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayInputStream bais = new ByteArrayInputStream( data );

		GZIPInputStream zis = new GZIPInputStream( bais );
		byte[] tmpBuffer = new byte[256];
		int n;
		while((n = zis.read( tmpBuffer )) >= 0)
			baos.write( tmpBuffer, 0, n );
		zis.close();

		return baos.toByteArray();
	}
	
	/**
	 * Gets the next byte array int the input buffer.
	 * 
	 * @param buffer	the input buffer
	*/
	public static byte[] getNextBytes( final ByteBuffer buffer )
	{
		byte[] data = new byte[buffer.getInt()];
		buffer.get( data );
		return data;
	}
	
	/** 
     * Transforms a ByteBuffer object in a hexadecimal representation.
     * 
     * @param buffer     the ByteBuffer object
    */
    public static String byteBufferToHex( final ByteBuffer buffer )
    {
        return bytesToHex( buffer.array() );
    }

    /** 
	 * Transforms a byte array in a hexadecimal representation.
	 * 
	 * @param b		the byte array
	*/
	public static String bytesToHex( final byte[] b )
	{
		//return DatatypeConverter.printHexBinary( b );
		
		final char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
		StringBuffer buf = new StringBuffer( 64 );
		for(int j = 0; j < b.length; j++) {
			buf.append( hexDigit[(b[j] >> 4) & 0x0f] );
			buf.append( hexDigit[b[j] & 0x0f] );
		}
		
		return buf.toString();
	}
	
	
	
	/** 
	 * Transforms an hexadecimal representation in a byte array.
	 * 
	 * @param s		the hexadecimal representation
	*/
	/*public static byte[] hexToBytes( final String s )
	{
		//return DatatypeConverter.parseHexBinary( s );
		
		assert( s.length() >= 2 );
		
		int len = s.length();
		byte[] data = new byte[len / 2];
		for(int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit( s.charAt( i ), 16 ) << 4)
								 + Character.digit( s.charAt( i+1 ), 16 ));
		}
		
		return data;
	}*/
	
	/** 
	 * Transforms an hexadecimal representation in a ByteBuffer object.
	 * 
	 * @param s   the hexadecimal representation of the object
	*/
	public static ByteBuffer hexToByteBuffer( final String s )
	{
		return ByteBuffer.wrap( hexToBytes( s ) );
	}
	
	/** 
	 * Transforms an hexadecimal representation in a byte[] object.
	 * 
	 * @param s   the hexadecimal representation of the object
	*/
	public static byte[] hexToBytes( final String s )
	{
		//return DatatypeConverter.parseHexBinary( s );
		
		assert( s.length() >= 2 );
		
		int len = s.length();
		byte[] data = new byte[len / 2];
		for(int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit( s.charAt( i ), 16 ) << 4)
								 + Character.digit( s.charAt( i+1 ), 16 ));
		}
		
		return data;
	}
	
	/**
	 * Transforms an integer into a byte array.
	 * 
	 * @param value		the int value
	*/
	public static byte[] intToByteArray( final int value )
	{
		return new byte[] {
				(byte) (value >>> 24),
				(byte) (value >>> 16),
				(byte) (value >>> 8),
				(byte) value
			};
	}
	
	/**
	 * Transforms a byte array into an integer number.
	 * 
	 * @param data		the byte array
	*/
	public static int byteArrayToInt( final byte[] data )
	{
		int value = 0;
		for(int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (data[i] & 0x000000FF) << shift;
		}
		
		return value;
	}
	
	/**
	 * Transforms a long into a byte array.
	 * 
	 * @param value		the long value
	*/
	public static byte[] longToByteArray( final long value )
	{
		return new byte[] {
				(byte) (value >> 56),
				(byte) (value >> 48),
				(byte) (value >> 40),
				(byte) (value >> 32),
				(byte) (value >> 24),
				(byte) (value >> 16),
				(byte) (value >> 8),
				(byte) value
			};
	}

	/**
	 * Returns the long representation of a byte array.
	 * 
	 * @param key	the input key
	*/
	public static long bytesToLong( final byte[] key )
	{
		ByteBuffer wrapper = ByteBuffer.wrap( key );
		return wrapper.getLong();
	}

	/** 
	 * Parse a JSON file.
	 * 
	 * @param path	file location
	*/
	public static JSONObject parseJSONFile( final String path ) throws IOException, JSONException
	{
		BufferedReader file = new BufferedReader( new FileReader( path ) );
		StringBuilder content = new StringBuilder( 512 );
		String line;
		while((line = file.readLine()) != null)
			content.append( line.trim() );
		file.close();
		
		return new JSONObject( content.toString() );
	}
}