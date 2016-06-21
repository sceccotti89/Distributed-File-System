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
	private List<byte[]> objects;
	
	private static final long serialVersionUID = 5483699354525628260L;
	
	public MessageResponse()
	{
		this( (byte) 0x0 );
	}

	public MessageResponse( final byte response )
	{
		this( response, null );
	}
	
	public MessageResponse( final byte response, final List<byte[]> objects )
	{
		super( response );
		
		this.objects = objects;
	}
	
	public void addObject( final byte[] object )
	{
		if(objects == null)
		    objects = new ArrayList<>( 4 );
		
		objects.add( object );
	}
	
	public List<byte[]> getObjects()
	{
		return objects;
	}
}