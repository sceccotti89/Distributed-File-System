/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net.messages;

public class MessageRequest extends Message
{
	private String fileName;
	private byte[] data;
	private boolean startQuorum;
	private String destId;
	private String clientAddress;
	private String HintedHandoff;
	
	private static final long serialVersionUID = 307888610331132428L;
	
	public MessageRequest( final byte opType )
	{
		this( opType, null );
	}
	
	public MessageRequest( final byte opType, final String fileName )
	{
		this( opType, fileName, null );
	}
	
	public MessageRequest( final byte opType, final String fileName, final byte[] data )
	{
		this( opType, fileName, data, false );
	}
	
	public MessageRequest( final byte opType, final String fileName,
						   final byte[] data, final boolean startQuorum )
	{
		this( opType, fileName, data, startQuorum, null, null, null );
	}
	
	public MessageRequest( final byte opType, final String fileName,
			   			   final byte[] data, final boolean startQuorum,
			   			   final String destId, final String clientAddress,
			   			   final String HintedHandoff )
	{
		super( opType );
		
		this.fileName = fileName;
		this.data = data;
		this.startQuorum = startQuorum;
		this.destId = destId;
		this.clientAddress = clientAddress;
		this.HintedHandoff = HintedHandoff;
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	/**
	 * Returns the payload of the message.
	*/
	public byte[] getData()
	{
		return data;
	}
	
	public boolean startQuorum()
	{
		return startQuorum;
	}
	
	public String getDestId()
	{
		return destId;
	}
	
	public String getClientAddress()
	{
		return clientAddress;
	}
	
	public String getHintedHandoff()
	{
		return HintedHandoff;
	}

	public void setClientAddress( final String address )
	{
		clientAddress = address;
	}
}