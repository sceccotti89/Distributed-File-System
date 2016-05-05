/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.util.Scanner;

import distributed_fs.net.messages.Message;

public class Client
{
	private static final Scanner SCAN = new Scanner( System.in );
	
	public static void main( final String args[] )
	{
		checkInput();
		
		try {
			DFSService service = new DFSService();
			if(service.start()) {
				//service.get( "./Resources/chord_sigcomm.pdf" );
				//service.put( "./Resources/chord_sigcomm.pdf" );
				//service.delete( "chord_sigcomm.pdf" );
				Operation op = checkInput();
				switch( op.opType ) {
					case( Message.GET ):
						service.get( op.file );
						break;
					case( Message.PUT ):
						service.put( op.file );
						break;
					case( Message.DELETE ):
						service.delete( op.file );
						break;
				}
				
				service.shutDown();
			}
		}
		catch( Exception e ) {
			
		}
	}
	
	private static Operation checkInput()
	{
		int i = 0;
		while(i < 1) {
			String line = SCAN.nextLine();
			
			if(line.startsWith( "GET" ))
				;
			else if(line.startsWith( "PUT" ))
				;
			else if(line.startsWith( "DELETE" ))
				;
			else
				System.out.println( "[CLIENT] Command '" + line + "' unknown." );
		}
		
		return null;
	}
	
	private static class Operation
	{
		String file;
		byte opType;
		
		public Operation( final String file, final byte opType )
		{
			this.file = file;
			this.opType = opType;
		}
	}
}