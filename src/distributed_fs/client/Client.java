/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.cli.ParseException;

import distributed_fs.files.DistributedFile;
import distributed_fs.net.messages.Message;
import distributed_fs.utils.CmdLineParser;
import gossiping.GossipMember;

public class Client
{
	public static final BufferedReader SCAN = new BufferedReader( new InputStreamReader( System.in ) );
	//private static final String PUT_REGEX	 = "put(?:\\s+\\w*)?";
	//private static final String GET_REGEX	 = "get(?:\\s+\\w*)?";
	//private static final String DELETE_REGEX = "delete[ ]{0,1}";
	private static final String FILE_REGEX	 = "([^ !$`&*()+]|(\\[ !$`&*()+]))+";
	
	public static void main( final String args[] ) throws ParseException
	{
		// TODO inserire anche il resource location e il database location tra i possibili input
		
		List<GossipMember> members = null;
		CmdLineParser.parseArgs( args );
		members = CmdLineParser.getNodes( "n" );
		
		//checkInput();
		DFSService service = null;
		
		try {
			service = new DFSService( members, null, null );
			if(service.start()) {
				while(true) {
					Operation op = checkInput( service );
					if(op == null)
						break;
					
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
				}
			}
		}
		catch( Exception e ) {
			// Ignored.
			//e.printStackTrace();
		}
		
		if(service != null)
			service.shutDown();
		
		System.exit( 0 );
	}
	
	private static Operation checkInput( final DFSService service )
	{
		String input = null;
		
		while(true) {
			System.out.print( "[CLIENT] " );
			
			try{
				// Wait until we have data to complete a readLine()
				while(!SCAN.ready()) {
					if(service.isClosed())
						return null;
					
					Thread.sleep( 200 );
				}
				
				input = SCAN.readLine();
			}
			catch( IOException | InterruptedException e ){
				break;
			}
			
			String command = input.toLowerCase();
			
			//if(command.matches( GET_REGEX )) {
			if(command.startsWith( "get" ) || command.startsWith( "get " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( input, 4 );
				if(file != null)
					return new Operation( file, Message.GET );
			}
			//else if(command.matches( PUT_REGEX )) {
			else if(command.startsWith( "put" ) || command.startsWith( "put " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( input, 4 );
				if(file != null)
					return new Operation( file, Message.PUT );
			}
			//else if(command.matches( DELETE_REGEX )) {
			else if(command.startsWith( "delete" ) || command.startsWith( "delete " )) {
				if(command.length() <= 7) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( input, 7 );
				if(file != null)
					return new Operation( file, Message.DELETE );
			}
			else if(command.equals( "list" )) {
				// List all the files present in the database.
				List<DistributedFile> files = service.listFiles();
				for(DistributedFile file : files) {
					if(!file.isDeleted())
						System.out.println( "[CLIENT] " + file.getName() );
				}
			}
			else
				System.out.println( "[CLIENT] Command '" + input + "' unknown." );
		}
		
		return null;
	}
	
	private static String getFile( final String command, final int offset )
	{
		String file = command.substring( offset );
		//System.out.println( "FILE: " + file );
		if(file.matches( FILE_REGEX ))
			return file;
		else
			System.out.println( "[CLIENT] Command error: you can specify only one file at the time." );
		
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