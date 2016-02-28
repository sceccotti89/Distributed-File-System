/**
 * @author Stefano Ceccotti
*/

package distributed_chord.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.json.JSONException;
import org.json.JSONObject;

import distributed_chord.overlay.LocalChordNode;
import distributed_chord.utils.ChordUtils;

public class FileManagementThread extends Thread
{
	/** The associated local node */
	private LocalChordNode node;
	/** Values of the associated node */
	private NavigableMap<Long, String> values;
	/** Maximum value on the Chord ring */
	private long max_value;
	
	/** Port used to send/receive a file */
	private static final short TCP_PORT = 7535;
	
	public FileManagementThread( final LocalChordNode node, final long max_value )
	{
		this.node = node;
		this.max_value = max_value;
		
		values = new ConcurrentSkipListMap<Long, String>();
		
		// load the values present in the resource folder
		String list[] = new File( ChordUtils.RESOURCE_LOCATION ).list();
		int length = list.length;
		for(int i = 0; i < length; i++)
			saveFile( list[i] );
		
		ChordUtils.LOGGER.info( "Passive Thread successfully initialized" );
	}
	
	@Override
	public void run()
	{
		ServerSocket servsock = null;
		InputStream is = null;
		BufferedReader in = null;
		Socket socket = null;
		
		try
		{
			servsock = new ServerSocket( TCP_PORT );
			
			ChordUtils.LOGGER.info( "Passive Thread launched" );
			
			while(true)
			{
				socket = servsock.accept();
				ChordUtils.LOGGER.info( "Received a new TCP connection..." );
				
				is = socket.getInputStream();
				in = new BufferedReader( new InputStreamReader( is ) );
				
				char type = in.readLine().charAt( 0 );
				switch( type )
				{
					case( ChordUtils.CHORD_WRITE ):
						receiveFiles( in, is );
						break;
					
					case( ChordUtils.CHORD_READ ):
						// TODO ottenere l'indirizzo del mittente e i/il file da trasmettere
						
						//sendFiles(  );
						break;
					
					case( ChordUtils.CHORD_DELETE ):
						deleteFiles( in );
						break;
				}
				
				in.close();
				socket.close();
			}
		}
		catch( IOException e )
	    {
			//e.printStackTrace();
		}
		
		try
		{
			if(servsock != null) servsock.close();
			if(is != null) is.close();
			if(socket != null) socket.close();
		}
		catch( IOException e1 )
	    {
			//e1.printStackTrace();
		}
	}
	
	/** Receive the input files
	 * 
	 * @param in	the buffer reader
	 * @param is	the input stream
	*/
	private void receiveFiles( final BufferedReader in, final InputStream is ) throws IOException
	{
		int num_files = Integer.parseInt( in.readLine() );
		
		for(int i = 0; i < num_files; i++)
		{
			// receive the name and the size of the file
			String file = in.readLine();
			final int file_size = Integer.parseInt( in.readLine() );
			
			ChordUtils.LOGGER.info( "File name: " + file + ", size: " + file_size + " bytes" );
			
			// receive the content file
			byte bytes[] = new byte[file_size];
			
			int current = 0;
			while(current < file_size)
			{
				int bytesRead = is.read( bytes, current, (bytes.length - current) );
				if(bytesRead >= 0)
					current += bytesRead;
				else
					break;
			}
			
			if(current < file_size)
				ChordUtils.LOGGER.error( "Error during download of the file " + file );
			else
			{
				ChordUtils.deserializeObject( ChordUtils.RESOURCE_LOCATION + file, bytes );
				ChordUtils.LOGGER.info( "File " + file + " downloaded" );
			}
			
			saveFile( file );
		}
	}
	
	/** Save an object
	 * 
	 * @param file	name of the file to save
	*/
	private void saveFile( final String file )
	{
		long ID = ChordUtils.getID( file );
		ChordUtils.LOGGER.info( "[FILE]: " + file + ", ID: " + ID );
		values.put( ID, file );
	}
	
	/** Delete all the input files
	 * 
	 * @param in	the input stream
	*/
	private void deleteFiles( final BufferedReader in ) throws IOException
	{
		int size = Integer.parseInt( in.readLine() );
		for(int i = 0; i < size; i++)
			removeFile( in.readLine() );
	}
	
	/** Remove an object
	 * 
	 * @param file	name of the file to remove
	*/
	public void removeFile( final String file )
	{
		long ID = ChordUtils.getID( file );
		ChordUtils.LOGGER.info( "[FILE]: " + file + ", ID: " + ID );
		values.remove( ID );
		
		new File( ChordUtils.RESOURCE_LOCATION + file ).delete();
	}
	
	/**  */
	public boolean sendDeleteFiles( final List<String> files, final String address )
	{
		Socket socket = null;
		PrintWriter out = null;
		boolean complete = true;
		
		try
		{
			socket = new Socket( address, TCP_PORT );
			out = new PrintWriter( socket.getOutputStream(), true );
			
			// send the type of message
			out.println( ChordUtils.CHORD_DELETE );
			
			// send the number of files to send
			int size = files.size();
			out.println( size );
			
			StringBuilder builder = new StringBuilder( 256 );
			for(int i = 0; i < size; i++)
				builder.append( values.get( i ) + "\n" );
			
			out.println( builder.toString() );
		}
		catch( IOException e )
		{
			//e.printStackTrace();
			complete = false;
		}
		
		try{
			if(out != null) out.close();
	        if(socket != null) socket.close();
		}
		catch( IOException e ){}
		
		return complete;
	}
	
	/** Send the keys associated to the input ID
	 * 
	 * @param ID		input identifier
	 * @param address	source IP address
	*/
	public void sendFiles( final long ID, final String address )
	{
		try
		{
			List<String> result = new ArrayList<>();
			
			String predecessor = node.getPredecessor( node.getID() );
			long predID = (predecessor == null) ? -1 : new JSONObject( node.getPredecessor( node.getID() ) ).getLong( ChordUtils.JSON_ID );
			
			// get the keys to send
			if(ID > node.getID())
			{
				if(predecessor == null)
					result.addAll( values.subMap( node.getID(), false, ID, true ).values() );
				else
					result.addAll( values.subMap( predID, false, ID, true ).values() );
			}
			else
			{
				if(predecessor == null || predID > ID)
				{
					result.addAll( values.subMap( node.getID(), false, max_value, false ).values() );
					result.addAll( values.subMap( 0L, true, ID, true ).values() );
				}
				else
					result.addAll( values.subMap( predID, false, ID, true ).values() );
			}
			
			System.out.println( "FILES TO SEND: " + result );
			
			if(result.size() > 0)
				sendFiles( result, null, address );
		}
		catch( IOException | JSONException e )
		{
			//e.printStackTrace();
		}
	}
	
	/** Send the list of files to the destination address
	 * 
	 * @param files		list of files
	 * @param l_bytes	list of bytes (of the file); it can be null
	 * @param address	destination IP address
	 * 
	 * @return TRUE if the files are successfully transmitted, FALSE otherwise
	*/
	public boolean sendFiles( final List<String> files, List<byte[]> l_bytes, final String address )
	{
		OutputStream os = null;
		Socket socket = null;
		boolean complete = true;
		
		try
		{
			socket = new Socket( address, TCP_PORT );
			os = socket.getOutputStream();
			PrintWriter out = new PrintWriter( os, true );
			
			// send the type of message
			out.println( ChordUtils.CHORD_WRITE );
			
			// send the number of files to send
			int size = files.size();
			out.println( size );
			
			for(int i = 0; i < size; i++)
			{
				// get the file name
				String file = values.get( i );
				
				byte bytes[] = (l_bytes == null) ? null : l_bytes.get( i );
				if(bytes == null)
					bytes = ChordUtils.serializeObject( file );
				int length = bytes.length;
				
				// send name and size of the file
				out.println( file + "\n" + length );
				
				// send the file
				ChordUtils.LOGGER.info( "Sending " + file + "(" + length + " bytes)..." );
				
				os.write( bytes, 0, length );
				os.flush();
				
				ChordUtils.LOGGER.info( "File transmitted" );
			}
		}
		catch( IOException e )
		{
			//e.printStackTrace();
			complete = false;
		}
		
		try{
	        if(os != null) os.close();
	        if(socket != null) socket.close();
		}
		catch( IOException e ){}
		
		return complete;
	}
}