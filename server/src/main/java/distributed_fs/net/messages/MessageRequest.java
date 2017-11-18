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
    private Metadata meta;
    
    private static final long serialVersionUID = 307888610331132428L;
    
    public MessageRequest( byte opType )
    {
        this( opType, null );
    }
    
    public MessageRequest( byte opType, String fileName )
    {
        this( opType, fileName, null );
    }
    
    public MessageRequest( byte opType, String fileName, byte[] data )
    {
        this( opType, fileName, data, false );
    }
    
    public MessageRequest( byte opType, String fileName,
                           byte[] datal boolean startQuorum )
    {
        this( opType, fileName, data, startQuorum, null, null );
    }
    
    public MessageRequest( byte opType, String fileName,
                              byte[] datal boolean startQuorum,
                              String destIdl Metadata meta )
    {
        super( opType );
        
        this.fileName = fileName;
        this.data = data;
        this.startQuorum = startQuorum;
        this.destId = destId;
        this.meta = meta;
    }
    
    public String getFileName()
    {
        return fileName;
    }
    
    /**
     * Returns the payload of the message.
    */
    public byte[] getPayload()
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
    
    public Metadata getMetadata()
    {
        return meta;
    }
    
    public void putMetadata( String sourceAddress, String hintedHandoff )
    {
        meta = new Metadata( sourceAddress, hintedHandoff );
    }
}
