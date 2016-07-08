/**
 * @author Stefano Ceccotti
*/

package client;

import client.manager.SystemSimulation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.ParseException;

import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.storage.DFSDatabase.DBListener;
import distributed_fs.storage.DistributedFile;
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
	
	private SystemSimulation sim;
	
	private DFSService service = null;
	
	private static final String[] COMMANDS = new String[]{ "put", "get", "delete", "list", "enableLB", "disableLB", "help", "exit" };
	public static final BufferedReader SCAN = new BufferedReader( new InputStreamReader( System.in ) );
	private static final String FILE_REGEX	 = "([^ !$`&*()+]|(\\[ !$`&*()+]))+";
	
	
	
	public static void main( final String args[] ) throws ParseException, IOException, DFSException
    {
	    ClientArgsParser.parseArgs( args );
        if(ClientArgsParser.hasOnlyHelpOptions())
            return;
        
        String ipAddress = ClientArgsParser.getIpAddress();
        int port = ClientArgsParser.getPort();
        String resourceLocation = ClientArgsParser.getResourceLocation();
        String databaseLocation = ClientArgsParser.getDatabaseLocation();
        List<GossipMember> members = ClientArgsParser.getNodes();
        boolean localEnv = ClientArgsParser.isLocalEnv();
        new Client( ipAddress, port, resourceLocation, databaseLocation, members, localEnv );
    }
	
	public Client( final String ipAddress,
	               final int port,
	               final String resourceLocation,
	               final String databaseLocation,
	               List<GossipMember> members,
	               final boolean localEnv ) throws ParseException, IOException, DFSException
	{
	    if(localEnv) {
	        // Start some nodes to simulate the distributed system,
	        // but performed in a local environment.
	        System.out.println( "Starting the local environment..." );
	        sim = new SystemSimulation( ipAddress, members );
	        if(members == null)
	            members = sim.getNodes();
	    }
	    
		try {
			service = new DFSService( ipAddress, port, true, members,
			                          resourceLocation, databaseLocation, this );
			
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
            
			if(service.start()) {
				System.out.println( "[CLIENT] Type 'help' for commands informations." );
				while(!service.isClosed()) {
					Operation op = checkInput();
					if(op == null || service.isClosed())
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
			e.printStackTrace();
		}
		
		//if(service != null)
			//service.shutDown();
		if(sim != null)
		    sim.close();
		
		System.exit( 0 );
	}
	
	private Operation checkInput() throws DFSException
	{
		String command = null;
		
		while(true) {
			try{
				command = reader.readLine( "[CLIENT] " );
				try {
    				if(command.isEmpty())
    				    continue;
				}
				catch( NullPointerException e ) {
				    // Exception raised when the service is closed.
				    break;
				}
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
			else if(command.trim().equals( "disableLB" )) {
			    service.setUseLoadBalancers( false );
			}
			else if(command.trim().equals( "enableLB" )) {
			    service.setUseLoadBalancers( true );
            }
			else if(command.trim().equals( "list" )) {
				// Put an empty line between the command and the list of files.
				System.out.println();
				// List all the files present in the database.
				List<DistributedFile> files = service.listFiles();
				for(DistributedFile file : files) {
					if(!file.isDeleted())
						System.out.println( "  " + file.getName() );
				}
			}
			else if(command.trim().equals( "help" ))
				printHelp();
			else if(command.trim().equals( "exit" ))
				break;
			else {
				System.out.println( "[CLIENT] Command '" + command + "' unknown." );
				System.out.println( "[CLIENT] Type 'help' for more informations." );
			}
		}
		
		return null;
	}
	
	private static String getFile( final String command, final int offset )
	{
		String file = command.substring( offset ).trim();
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
				+ "  put \"file_name\" - send a file present in the database to a remote one.\n"
				+ "  get \"file_name\" - get a file present in the remote nodes.\n"
				+ "  delete \"file_name\" - delete a file present on database and in remote nodes.\n"
				+ "  list - to print a list of all the files present in the database.\n"
				+ "  enableLB - enable the utilization of the remote LoadBalancer nodes.\n"
                + "  diableLB - disable the utilization of the remote LoadBalancer nodes.\n"
                + "  help - to open this helper.\n"
				+ "  exit - to close the service.\n\n"
				+ "  You can also use the auto completion, to complete faster your commands.\n"
				+ "  Try it with the [TAB] key." );
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
