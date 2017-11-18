/**
 * @author Stefano Ceccotti
*/

package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONObject;

import client.manager.SystemSimulation;
import distributed_fs.exception.DFSException;
import distributed_fs.net.messages.Message;
import distributed_fs.storage.DBManager.DBListener;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;
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
    
    private static final BufferedReader SCAN = new BufferedReader( new InputStreamReader( System.in ) );
    
    // List of client's commands.
    private static final String CMD_REGEX = "[\\t\\s]*";
    private static final String[] COMMANDS = new String[]{ "put", "get", "delete", "list",
                                                           "enableLB", "disableLB", "enableSync", "disableSync",
                                                           "help", "exit" };
    //private static final String FILE_REGEX = "([^ !$`&*()+]|(\\[ !$`&*()+]))+";
    
    
    
    
    public static void main( String args[] )
            throws ParseException, IOException, DFSException, InterruptedException
    {
        ClientArgsParser.parseArgs( args );
        if(ClientArgsParser.hasOnlyHelpOption())
            return;
        
        JSONObject configFile = ClientArgsParser.getConfigurationFile();
        if(configFile != null && !ClientArgsParser.hasOnlyFileOption()) {
            throw new ParseException( "If you have defined a configuration file " +
                                      "the other options are not needed." );
        }
        
        if(configFile != null)
            fromJSONFile( configFile );
        else {
            String ipAddress = ClientArgsParser.getIpAddress();
            int port = ClientArgsParser.getPort();
            String resourceLocation = ClientArgsParser.getResourceLocation();
            String databaseLocation = ClientArgsParser.getDatabaseLocation();
            List<GossipMember> members = ClientArgsParser.getNodes();
            boolean localEnv = ClientArgsParser.isLocalEnv();
            new Client( ipAddress, port, resourceLocation, databaseLocation, members, localEnv );
        }
    }
    
    private static void fromJSONFile( JSONObject configFile ) throws ParseException, IOException, DFSException, InterruptedException
    {
        String address = configFile.has( "Address" ) ? configFile.getString( "Address" ) : null;
        int port = configFile.has( "Port" ) ? configFile.getInt( "Port" ) : 0;
        String resourcesLocation = configFile.has( "ResourcesLocation" ) ? configFile.getString( "ResourcesLocation" ) : null;
        String databaseLocation = configFile.has( "DatabaseLocation" ) ? configFile.getString( "DatabaseLocation" ) : null;
        List<GossipMember> members = getStartupMembers( configFile );
        boolean localEnv = configFile.has( "LocalEnv" ) ? configFile.getBoolean( "LocalEnv" ) : false;
        
        new Client( address, port, resourcesLocation, databaseLocation, members, localEnv );
    }
    
    /**
     * Gets the list of members present in the configuration file.
     * 
     * @param configFile    the configuration file
    */
    private static List<GossipMember> getStartupMembers( JSONObject configFile )
    {
        List<GossipMember> members = null;
        if(!configFile.has( "members" ))
            return members;
        
        JSONArray membersJSON = configFile.getJSONArray( "members" );
        int length = membersJSON.length();
        members = new ArrayList<>( length );
        
        for(int i = 0; i < length; i++) {
            JSONObject memberJSON = membersJSON.getJSONObject( i );
            String host = memberJSON.getString( "host" );
            int Port = memberJSON.getInt( "port" );
            String id = DFSUtils.getNodeId( 1, host + ":" + Port );
            RemoteGossipMember member = new RemoteGossipMember( host, Port, id, 0, memberJSON.getInt( "type" ) );
            members.add( member );
        }
        
        return members;
    }
    
    public Client( String ipAddress,
                   int port,
                   String resourceLocation,
                   String databaseLocation,
                   List<GossipMember> members,
                   boolean localEnv ) throws ParseException, IOException, DFSException, InterruptedException
    {
        if(localEnv) {
            // Start some nodes to simulate the distributed system,
            // but performed in a local environment.
            System.out.println( "Starting the pseudo-distributed environment..." );
            sim = new SystemSimulation( ipAddress, 1, members );
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
            reader.setDefaultPrompt( "[CLIENT] " );
            
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
                            if(checkDeleteConfirm())
                                service.delete( op.file );
                            break;
                    }
                }
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        
        if(service != null)
            service.shutDown();
        if(sim != null)
            sim.close();
        
        System.exit( 0 );
    }
    
    /**
     * Checks and retrieve the user input.
    */
    private Operation checkInput() throws DFSException
    {
        String command = null;
        
        while(true) {
            try{
                command = readCommand();
            } catch( IOException e ){
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
            else if(command.matches( CMD_REGEX + "disableLB" + CMD_REGEX ))
                service.setUseLoadBalancers( false );
            else if(command.matches( CMD_REGEX + "enableLB" + CMD_REGEX ))
                service.setUseLoadBalancers( true );
            else if(command.matches( CMD_REGEX + "disableSync" + CMD_REGEX ))
                service.setSyncThread( false );
            else if(command.matches( CMD_REGEX + "enableSync" + CMD_REGEX ))
                service.setSyncThread( true );
            else if(command.matches( CMD_REGEX + "list" + CMD_REGEX )) {
                // Put an empty line between the command and the list of files.
                System.out.println();
                // List all the files present in the database.
                List<DistributedFile> files = service.listFiles();
                for(DistributedFile file : files) {
                    if(!file.isDeleted())
                        System.out.println( "  " + file.getName() );
                }
            }
            else if(command.matches( CMD_REGEX + "help" + CMD_REGEX ))
                printHelp();
            else if(command.matches( CMD_REGEX + "exit" + CMD_REGEX ))
                break;
            else {
                System.out.println( "[CLIENT] Command '" + command + "' unknown." );
                System.out.println( "[CLIENT] Type 'help' for more informations." );
            }
        }
        
        return null;
    }
    
    /**
     * Returns the command inserted by the user.
     * 
     * @return the command, or {@code null} if the input reader has been closed
    */
    private String readCommand() throws IOException
    {
        String command = null;
        
        while(true) {
            if(service.isReconciling() || !SCAN.ready()) {
                try { Thread.sleep( 50 ); }
                catch( InterruptedException e ) {}
                continue;
            }
            
            command = reader.readLine();
            try {
                if(!command.isEmpty())
                    break;
            } catch( NullPointerException e ) {
                // Exception raised when the service is closed.
                break;
            }
        }
        
        return command;
    }
    
    /**
     * Returns the name of the file contained in the command.
     * 
     * @param command  the actual command
     * @param offset   offset from which start the search
     * 
     * @return the name of the file, or {@code null} if some errors are present
    */
    private static String getFile( String command, int offset )
    {
        // Remove the initial white spaces and tabs.
        while(offset < command.length() &&
             (command.charAt( offset ) == ' ' ||
              command.charAt( offset ) == '\t')) {
            offset++;
        }
        
        // Remove the last white spaces and tabs.
        int index = command.length() - 1;
        while(index >= offset &&
             (command.charAt( index ) == ' ' ||
              command.charAt( index ) == '\t')) {
            index--;
        }
        
        String file = command.substring( offset, index + 1 );
        if(file.startsWith( "\"" ) && file.endsWith( "\"" ))
            file = file.substring( 1, file.length() - 1 );
        else {
            if(file.contains( " " ) || file.contains( "\t" )) {
                System.out.println( "[CLIENT] Command error: you can specify only one file at the time." );
                System.out.println( "[CLIENT] If the file contains one or more spaces, put it inside the \"\" boundaries." );
                return null;
            }
        }
        
        return file;
    }
    
    /**
     * Checks the confirm to remove a file.
     * 
     * @return {@code true} to confirm, {@code false} otherwise
    */
    private boolean checkDeleteConfirm() throws IOException
    {
        System.out.print( "[CLIENT] Do you really want to delete the file (y/n)? " );
        String line = reader.readLine( "" );
        return line.isEmpty() || line.equalsIgnoreCase( "y" ) || line.equalsIgnoreCase( "yes" );
    }
    
    @Override
    public void dbEvent( String fileName, byte code )
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
                + "  disableLB - disable the utilization of the remote LoadBalancer nodes.\n"
                + "  enableSync - enable the utilization of the synchronizer thread.\n"
                + "  disableSync - disable the utilization of the synchronizer thread.\n"
                + "  help - to open this helper.\n"
                + "  exit - to close the service.\n\n"
                + "  You can also use the auto completion, to complete faster your commands.\n"
                + "  Try it with the [TAB] key." );
    }
    
    /**
     * Class used to represent user input commands.
    */
    private static class Operation
    {
        String file;
        byte opType;
        
        public Operation( String file, byte opType )
        {
            this.file = file;
            this.opType = opType;
        }
    }
}
