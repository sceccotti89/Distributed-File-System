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
import distributed_fs.net.messages.Message;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.ArgumentsParser;
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
	private static final String FILE_REGEX	 = "([^ !$`&*()+]|(\\[ !$`&*()+]))+";
	
	public static void main( final String args[] ) throws ParseException
	{
		new Client( args );
	}
	
	public Client( String[] args ) throws ParseException
	{
		// Parse the command options.
		ArgumentsParser.parseArgs( args );
		
		String ipAddress = ArgumentsParser.getIpAddress();
		int port = ArgumentsParser.getPort();
		boolean useLoadBalancers = ArgumentsParser.getLoadBalancers();
		String resourceLocation = ArgumentsParser.getResourceLocation();
		String databaseLocation = ArgumentsParser.getDatabaseLocation();
		List<GossipMember> members = ArgumentsParser.getNodes();
		
		try {
			service = new DFSService( ipAddress, port, useLoadBalancers, members,
			                          resourceLocation, databaseLocation, this );
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
			try{
				command = reader.readLine( "[CLIENT] " );
				if(command.isEmpty())
				    continue;
			}
			catch( IOException e ){
				e.printStackTrace();
				break;
			}
			
			if(command.startsWith( "get" ) || command.startsWith( "get " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 4 );
				if(file != null)
					return new Operation( file, Message.GET );
			}
			else if(command.startsWith( "put" ) || command.startsWith( "put " )) {
				if(command.length() <= 4) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 4 );
				if(file != null)
					return new Operation( file, Message.PUT );
			}
			else if(command.startsWith( "delete" ) || command.startsWith( "delete " )) {
				if(command.length() <= 7) {
					System.out.println( "[CLIENT] Command error: you must specify the file." );
					continue;
				}
				
				String file = getFile( command, 7 );
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
			else if(command.trim().equalsIgnoreCase( "help" ))
				printHelp();
			else if(command.trim().equalsIgnoreCase( "exit" ))
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
		if(code == Message.GET)
			dbFiles.add( fileName );
		else // DELETE
			dbFiles.remove( fileName );
		
		// Put the name of file present in the database.
		completors.removeLast();
		completors.addLast( new SimpleCompletor( dbFiles.toArray( new String[]{} ) ) );
		
		ArgumentCompletor aComp = new ArgumentCompletor( completors );
		reader.addCompletor( aComp );
		reader.removeCompletor( completor );
		completor = aComp;
	}
	
	private void printHelp()
	{
		System.out.println( "[CLIENT] Usage:\n"
				+ " put \"file_name\" - send a file present in the database to a remote one.\n"
				+ " get \"file_name\" - get a file present in the remote database.\n"
				+ " delete \"file_name\" - delete a file present in the database and in a remote one.\n"
				+ " list - to print a list of all the files present in the database.\n"
				+ " exit - to close the service.\n"
				+ " help - to open this helper.\n\n"
				+ " You can also use the autocompletion, to complete faster your commands.\n"
				+ " Try it with the [TAB] key." );
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