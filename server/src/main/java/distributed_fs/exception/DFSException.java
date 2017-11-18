/**
 * @author Stefano Ceccotti
*/

package distributed_fs.exception;

public class DFSException extends Exception
{
    private static final long serialVersionUID = 4411181177485645071L;

    public DFSException()
    {
        super();
    }
    
    public DFSException( String message )
    {
        super( message );
    }
    
    public DFSException( String s, Throwable t )
    {
        super( s, t );
    }

    public DFSException( Throwable t )
    {
        super( t );
    }
}
