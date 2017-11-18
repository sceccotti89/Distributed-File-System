/**
 * @author Stefano Ceccotti
*/

package distributed_fs.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import distributed_fs.overlay.DFSNode;
import distributed_fs.utils.resources.ResourceLoader;

public class DFSUtils
{
    /** Hash function used to compute the identifier of a node. */
    public static final HashFunction _hash = Hashing.sha1();
    
    /** Gossiping configuration path. */
    public static final String GOSSIP_CONFIG = "Settings/GossipSettings.json";
    
    /** Logger configuration path. */
    public static final String LOG_CONFIG = "Settings/log4j.properties";
    
    /** Default port used to send/receive data in the service. */
    public static final int SERVICE_PORT = 9000;
    
    /** Decides whether the configurations are initialized. */
    public static boolean initConfig = false;
    
    /**
     * Returns the identifier associated to the node.
     * 
     * @param virtualNode    the virtual node instance
     * @param address        the network address of the object
    */
    public static String getNodeId( int virtualNode, String address )
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
     * @param object    the given object. It, and all of its superclasses,
     *                     must implement the {@link java.io.Serializable} interface
    */
    public static <S extends Serializable> String getId( S object )
    {
        byte[] bucketNameInBytes = serializeObject( object );
        return bytesToHex( _hash.hashBytes( bucketNameInBytes ).asBytes() );
    }
    
    /**
     * Returns the number of virtual nodes that can be managed,
     * based on the capabilities of this machine.
    */
    public static short computeVirtualNodes() throws IOException
    {
        short virtualNodes = 2;
        
        Runtime runtime = Runtime.getRuntime();
        
        // Total number of processors or cores available to the JVM.
        int cores = runtime.availableProcessors();
        DFSNode.LOGGER.debug( "Available processors: " + cores + ", CPU nodes: " + (cores * 4) );
        virtualNodes = (short) (virtualNodes + (cores * 4));
        
        // Size of the RAM.
        long RAMsize = 0L;
        String OS = System.getProperty( "os.name" ).toLowerCase();
        if(OS.indexOf( "win" ) >= 0) {
            // Windows command.
            ProcessBuilder pb = new ProcessBuilder( "wmic", "computersystem", "get", "TotalPhysicalMemory" );
            Process proc = pb.start();
            short count = 0;
            
            InputStream stream = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader( stream );
            BufferedReader br = new BufferedReader( isr );
            
            String line = null;
            while((line = br.readLine()) != null && ++count < 3);
            
            br.close();
            
            if(count != 3)
                return virtualNodes;
            
            //System.out.println( line );
            RAMsize = Long.parseLong( line.trim() );
        }
        else if(OS.indexOf( "nix" ) >= 0 || OS.indexOf( "nux" ) >= 0 || OS.indexOf( "aix" ) > 0) {
            // Linux command.
            ProcessBuilder pb = new ProcessBuilder( "less", "/proc/meminfo" );
            Process proc = pb.start();
            
            InputStream stream = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader( stream );
            BufferedReader br = new BufferedReader( isr );
            
            boolean found = false;
            String line = null;
            while((line = br.readLine()) != null) {
                if(line.startsWith( "MemTotal" )) {
                    found = true;
                    break;
                }
            }
            
            br.close();
            
            if(!found)
                return virtualNodes;
            
            Matcher matcher = Pattern.compile( "[0-9]+(.*?)[0-9]" ).matcher( line );
            matcher.find();
            // Multiply it by 1024 because the result is expressed in kBytes.
            RAMsize = Long.parseLong( line.substring( matcher.start(), matcher.end() ) ) * 1024;
        }
        else if(OS.indexOf( "mac" ) >= 0) {
            // MAC command.
            ProcessBuilder pb = new ProcessBuilder( "system_profiler", "|", "grep \"  Memory:\"" );
            Process proc = pb.start();
            
            InputStream stream = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader( stream );
            BufferedReader br = new BufferedReader( isr );
            
            String line = br.readLine();
            
            br.close();
            
            if(line != null) {
                int length = line.length();
                String size = line.substring( length - 2 );
                String dim  = line.trim().substring( 0, length - 2 );
                if(size.equalsIgnoreCase( "GB" ))
                    RAMsize = Long.parseLong( dim ) * 1073741824;
                else // KB??
                    RAMsize = Long.parseLong( dim ) * 1048576;
            }
            else
                return virtualNodes;
        }
        
        DFSNode.LOGGER.debug( "RAM size: " + RAMsize + ", RAM nodes: " + (RAMsize / 262144000) );
        virtualNodes = (short) (virtualNodes + (RAMsize / 262144000)); // Divide it by 250MBytes.
        
        DFSNode.LOGGER.debug( "Total nodes: " + virtualNodes );
        
        return virtualNodes;
    }
    
    /** 
     * Serializes an object.
     * 
     * @param obj    the object to serialize. It must implements the
     *                {@link java.io.Serializable} interface
     * 
     * @return the byte serialization of the object, if no error happens, null otherwise
    */
    public static <T extends Serializable> byte[] serializeObject( T obj )
    {
        //if(obj instanceof String)
        //    return ((String) obj).getBytes( StandardCharsets.UTF_8 );
        
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
     * @param data        bytes of the serialized object
     * 
     * @return the deserialization of the object,
     *            casted to the type specified in {@link T}
    */
    public static <T extends Serializable> T deserializeObject( byte data[] )
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
     * Generates a random file as string.
    */
    public static String generateRandomFile()
    {
        int size = (int) (Math.random() * 5) + 1; // Max 5 characters.
        StringBuilder builder = new StringBuilder( size );
        for(int i = 0; i < size; i++) {
            builder.append( (char) ((Math.random() * 57) + 65) );
        }
        
        return builder.toString();
    }
    
    /**
     * Compresses data using a GZIP compressor.
     * 
     * @param data     bytes to compress
     * 
     * @return the compressed bytes array
    */
    public static byte[] compressData( byte[] data )
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
     * @param data    the bytes to decompress
     * 
     * @return the decompressed bytes array
    */
    public static byte[] decompressData( byte[] data ) throws IOException
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
     * @param buffer    the input buffer
    */
    public static byte[] getNextBytes( ByteBuffer buffer )
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
    public static String byteBufferToHex( ByteBuffer buffer )
    {
        return bytesToHex( buffer.array() );
    }
    
    /** 
     * Transforms a byte array in a hexadecimal representation.
     * 
     * @param b        the byte array
    */
    public static String bytesToHex( byte[] b )
    {
        final char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
        StringBuffer buf = new StringBuffer( 64 );
        for(int j = 0; j < b.length; j++) {
            buf.append( hexDigit[(b[j] >> 4) & 0x0f] );
            buf.append( hexDigit[b[j] & 0x0f] );
        }
        
        return buf.toString();
    }
    
    /** 
     * Transforms an hexadecimal representation in a ByteBuffer object.
     * 
     * @param s   the hexadecimal representation of the object
    */
    public static ByteBuffer hexToByteBuffer( String s )
    {
        return ByteBuffer.wrap( hexToBytes( s ) );
    }
    
    /** 
     * Transforms an hexadecimal representation in a byte[] object.
     * 
     * @param s   the hexadecimal representation of the object
    */
    public static byte[] hexToBytes( String s )
    {
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
     * @param value        the int value
    */
    public static byte[] intToByteArray( int value )
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
     * @param data        the byte array
    */
    public static int byteArrayToInt( byte[] data )
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
     * @param value        the long value
    */
    public static byte[] longToByteArray( long value )
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
     * @param key    the input key
    */
    public static long bytesToLong( byte[] key )
    {
        ByteBuffer wrapper = ByteBuffer.wrap( key );
        return wrapper.getLong();
    }
    
    /** 
     * Parse a JSON file.
     * 
     * @param path    file location
    */
    public static JSONObject parseJSONFile( String path ) throws IOException
    {
        InputStream in = ResourceLoader.getResourceAsStream( path );
        BufferedReader file = new BufferedReader( new InputStreamReader( in ) );
        StringBuilder content = new StringBuilder( 512 );
        String line;
        while((line = file.readLine()) != null)
            content.append( line.trim() );
        file.close();
        
        try {
            JSONObject jFile = new JSONObject( content.toString() );
            return jFile;
        } catch( JSONException e ) {
            throw new RuntimeException( e );
        }
    }
}
