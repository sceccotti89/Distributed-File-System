/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net.messages;

import java.util.ArrayList;
import java.util.List;

public class MessageResponse extends Message
{
	//private String destHost;
	//private byte[] destId;
	private List<byte[]> files;
	
	private static final long serialVersionUID = 5483699354525628260L;
	
	public MessageResponse()
	{
		this( (byte) 0x0 );
	}

	public MessageResponse( final byte response )
	{
		this( response, null );
	}
	
	public MessageResponse( final byte response, final List<byte[]> files )
	{
		super( response );
		
		this.files = files;
	}
	
	public void addFile( final byte[] file )
	{
		if(files == null)
			files = new ArrayList<>();
		
		files.add( file );
	}
	
	public List<byte[]> getFiles()
	{
		return files;
	}
}