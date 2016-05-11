/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.ParseException;

import distributed_fs.client.DFSService.DBListener;
import distributed_fs.files.DistributedFile;
import distributed_fs.net.messages.Message;
import distributed_fs.utils.CmdLineParser;
import gossiping.GossipMember;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class Client implements DBListener
{
	private final Set<String> dbFiles = new HashSet<>( 64 );
	private ConsoleReader reader;
	private LinkedList<Completor> completors;
	private ArgumentCompletor completor;
	
	private DFSService service = null;
	
	private static final String[] COMMANDS = new String[]{ "put", "get", "delete", "list", "help", "exit" };
	public static final BufferedReader SCAN = new BufferedReader( new InputStreamReader( System.in ) );
	//private static final String PUT_REGEX	 = "put(?:\\s+\\w*)?";
	//private static final String GET_REGEX	 = "get(?:\\s+\\w*)?";
	//private static final String DELETE_REGEX = "delete[ ]{0,1}";
	private static final String FILE_REGEX	 = "([^ !$`&*()+]|(\\[ !$`&*()+]))+";
	
	public static void main( final String args[] ) throws ParseException
	{
		new Client( args );
	}
	
	public Client( final String[] args ) throws ParseException
	{
		// Parse the command options.
		CmdLineParser.parseArgs( args );
		
		String ipAddress = CmdLineParser.getIpAddress();
		int port = CmdLineParser.getPort();
		String resourceLocation = CmdLineParser.getResourceLocation();
		String databaseLocation = CmdLineParser.getDatabaseLocation();
		List<GossipMember> members = CmdLineParser.getNodes();
		
		//checkInput();
		
		try {
			service = new DFSService( ipAddress, port, members, resourceLocation, databaseLocation, this );
			if(service.start()) {
				for(DistributedFile file : service.listFiles())
					dbFiles.add( file.getName() );
				
				// Load the files in the completor.
				completors = new LinkedList<>();
				completors.addLast( new SimpleCompletor( COMMANDS ) );
				completors.addLast( new SimpleCompletor( dbFiles.toArray( new String[]{} ) ) );
				
				// Put the completor into the reader.
				completor = new ArgumentCompletor( completors );
				reader = new ConsoleReader();
				reader.setBellEnabled( false );
				reader.addCompletor( completor );
				
				while(!service.isClosed()) {
					Operation op = checkInput();
					if(op != null) {
						if(service.isClosed())
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
		}
		catch( Exception e ) {
			// Ignored.
			e.printStackTrace();
		}
		
		if(service != null)
			service.shutDown();
		
		System.exit( 0 );
	}
	
	private Operation checkInput()
	{
		String command = null;
		
		while(true) {
			//System.out.print( "[CLIENT] " );
			
			try{
				/*while ((command = reader.readLine( "[CLIENT] " )) != null) {
					
				}*/
				command = reader.readLine( "[CLIENT] " );
				
				// Wait until we have data to complete a readLine()
				/*while(!SCAN.ready()) {
					if(service.isClosed())
						return null;
					
					Thread.sleep( 200 );
				}
				
				command = SCAN.readLine();*/
			}
			catch( IOException e ){
				e.printStackTrace();
				break;
			}
			
			//String command = input.toLowerCase();
			
			//if(command.matches( GET_REGEX )) {
			if(command.startsWith( "get" ) || command.startsWith( "get " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 4 );
				//String file = getFile( input, 4 );
				if(file != null)
					return new Operation( file, Message.GET );
			}
			//else if(command.matches( PUT_REGEX )) {
			else if(command.startsWith( "put" ) || command.startsWith( "put " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 4 );
				//String file = getFile( input, 4 );
				if(file != null)
					return new Operation( file, Message.PUT );
			}
			//else if(command.matches( DELETE_REGEX )) {
			else if(command.startsWith( "delete" ) || command.startsWith( "delete " )) {
				if(command.length() <= 7) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 7 );
				//String file = getFile( input, 7 );
				if(file != null)
					return new Operation( file, Message.DELETE );
			}
			else if(command.equalsIgnoreCase( "list" )) {
				// Put an empty line between the command and the list of files.
				System.out.println( "[CLIENT]" );
				// List all the files present in the database.
				List<DistributedFile> files = service.listFiles();
				for(DistributedFile file : files) {
					if(!file.isDeleted())
						System.out.println( "[CLIENT] " + file.getName() );
				}
			}
			else if(command.equals( "exit" ))
				service.shutDown();
			else
				System.out.println( "[CLIENT] Command '" + command + "' unknown." );
		}
		
		return null;
	}
	
	private static String getFile( final String command, final int offset )
	{
		String file = command.substring( offset ).trim();
		System.out.println( "FILE: " + file );
		if(file.matches( FILE_REGEX ))
			return file;
		else
			System.out.println( "[CLIENT] Command error: you can specify only one file at the time." );
		
		return null;
	}
	
	@Override
	public void dbEvent( final String fileName, final byte code )
	{
		if(code == Message.PUT)
			dbFiles.add( fileName );
		else	// DELETE
			dbFiles.remove( fileName );
		
		// Put the name of file present in the database.
		completors.removeLast();
		completors.addLast( new SimpleCompletor( dbFiles.toArray( new String[]{} ) ) );
		
		ArgumentCompletor aComp = new ArgumentCompletor( completors );
		reader.addCompletor( aComp );
		reader.removeCompletor( completor );
		completor = aComp;
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