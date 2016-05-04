/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

public class Client
{
	public static void main( final String args[] )
	{
		try {
			if(args.length > 0 && Boolean.parseBoolean( args[0] )) {
				// TODO attivare la modalità interattiva
				
			}
			
			DFSService service = new DFSService();
			if(service.start()) {
				service.get( "./Resources/chord_sigcomm.pdf" );
				service.put( "./Resources/chord_sigcomm.pdf" );
				//service.delete( "chord_sigcomm.pdf" );
				
				service.shutDown();
			}
		}
		catch( Exception e ) {
			
		}
	}
}