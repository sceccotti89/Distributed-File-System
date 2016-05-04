/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net.messages;

public class MessageRequest extends Message
{
	private String fileName;
	private byte[] file;
	private boolean startQuorum;
	private String destId;
	private String clientAddress;//TODO decidere se mantenerlo o cambiare modello di comunicazione
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
	
	public MessageRequest( final byte opType, final String fileName, final byte[] file )
	{
		this( opType, fileName, file, false );
	}
	
	public MessageRequest( final byte opType, final String fileName,
						   final byte[] file, final boolean startQuorum )
	{
		this( opType, fileName, file, startQuorum, null, null, null );
	}
	
	public MessageRequest( final byte opType, final String fileName,
			   			   final byte[] file, final boolean startQuorum,
			   			   final String destId, final String clientAddress,
			   			   final String HintedHandoff )
	{
		super( opType );
		
		this.fileName = fileName;
		this.file = file;
		this.startQuorum = startQuorum;
		this.destId = destId;
		this.clientAddress = clientAddress;
		this.HintedHandoff = HintedHandoff;
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	public byte[] getFile()
	{
		return file;
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
}