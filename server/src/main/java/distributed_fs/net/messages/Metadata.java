/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net.messages;

import java.io.Serializable;

/**
 * The metadata informations exchanged during an operation.
*/
public class Metadata implements Serializable
{
    private final String clientAddress;
    private final String hintedHandoff;
    
    private static final long serialVersionUID = 3654063796036107949L;

    public Metadata( final String clientAddress, final String hintedHandoff )
    {
        this.clientAddress = clientAddress;
        this.hintedHandoff = hintedHandoff;
    }
    
    public String getClientAddress()
    {
        return clientAddress;
    }
    
    public String getHintedHandoff()
    {
        return hintedHandoff;
    }
}
