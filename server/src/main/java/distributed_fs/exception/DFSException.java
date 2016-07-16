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
	
	public DFSException( final String message )
	{
		super( message );
	}
	
	public DFSException( final String s, final Throwable t )
    {
        super( s, t );
    }

    public DFSException( final Throwable t )
    {
        super( t );
    }
}
