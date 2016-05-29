/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.json.JSONException;

import com.google.common.base.Preconditions;

import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.RemoteFile;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;
import gossiping.GossipMember;

public class DFSService extends DFSManager implements IDFSService
{
	private boolean initialized = false;
	//private boolean shutDown = false;
	
	private final Random random;
	private final DFSDatabase database;
	//private Timer syncClient;
	
	/** Decide whether the service is closed or not. */
	private boolean closed = false;
	private boolean testing = false;
	private DBListener listener;
	
	//private static final long KEEP_ALIVE = 1000;
	//private long timer = KEEP_ALIVE;
	//private long timestamp = System.currentTimeMillis();
	
	private static final Scanner SCAN = new Scanner( System.in );
	
	//private static final int CHECK_TIMER = 30000; // Time to wait before to check the database (30 seconds).
	
	public DFSService( final String ipAddress,
					   final int port,
					   final List<GossipMember> members,
					   final String resourcesLocation,
					   final String databaseLocation,
					   final DBListener listener ) throws IOException, JSONException, DFSException
	{
		super( ipAddress, members );
		
		this.port = (port <= 0) ? Utils.SERVICE_PORT : port;
		
		net = new TCPnet( address, this.port );
		net.setSoTimeout( 5000 );
		
		this.listener = listener;
		
		random = new Random();
		database = new DFSDatabase( resourcesLocation, databaseLocation, null );
		
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				shutDown();
				LOGGER.info( "Service has been shutdown..." );
			}
		});
	}
	
	/** Testing. */
	/*public DFSService( final String address,
					   final int port,
					   final List<GossipMember> members,
					   final String resourcesLocation,
					   final String databaseLocation ) throws IOException, JSONException, DFSException
	{
		super( null );
		
		testing = true;
		this.address = address;
		
		this.port = (port <= 0) ? Utils.SERVICE_PORT : port;
		
		net = new TCPnet( this.address, this.port );
		net.setSoTimeout( 5000 );
		
		random = new Random();
		database = new DFSDatabase( resourcesLocation, databaseLocation, null );
	}*/
	
	/**
	 * Starts the service.
	 * 
	 * @return {@code true} if the service has been started,
	 * 		   {@code false} otherwise.
	*/
	public boolean start() throws IOException
	{
		/*SynchronizeClient client = new SynchronizeClient();
		syncClient = new Timer( CHECK_TIMER, client );
		
		List<RemoteFile> files = getAllFiles();
		if(files == null) {
			LOGGER.info( "The system is not available. Retry later." );
			return false;
		}
		client.checkFiles( files );
		syncClient.start();*/
		//session = contactLoadBalancerNode();
		if(!contactLoadBalancerNode())
			return false;
		
		LOGGER.info( "System up." );
		return (initialized = true);
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	 * @throws InterruptedException 
	*/
	public DistributedFile getFile( String fileName ) throws InterruptedException
	{
		/*if(!fileName.startsWith( "./" ))
			fileName = "./" + fileName;
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName = fileName + "/";
		if(fileName.startsWith( database.getFileSystemRoot() ))
			fileName = fileName.substring( database.getFileSystemRoot().length() );*/
		fileName = checkFile( fileName, database.getFileSystemRoot() );
		
		DistributedFile file = database.getFile( Utils.getId( fileName ) );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Retrieves all the files stored in a random node of the network.
	*/
	/*private List<RemoteFile> getAllFiles()
	{
		LOGGER.info( "Synchonizing..." );
		
		List<RemoteFile> files = null;
		
		if(session == null || session.isClosed()) {
			session = contactLoadBalancerNode( false );
			if(session == null) {
				closed = true;
				return null;
			}
		}
		
		TCPSession remoteSession = null;
		
		try{
			//LOGGER.debug( "Sending message..." );
			sendGetAllMessage();
			//session.sendMessage( new byte[]{ Utils.GET_ALL }, true );
			//LOGGER.debug( "Request message sent" );
			if(!checkResponse( session, "GET_ALL", true ))
				throw new IOException();
			
			//session.close();
			
			if((remoteSession = waitRemoteConnection( "GET_ALL" )) == null)
				throw new IOException();
			
			/*LOGGER.info( "Waiting for the incoming files..." );
			byte[] data = session.receiveMessage();
			int size = Utils.byteArrayToInt( data );
			files = new ArrayList<>( size );
			for(int i = 0; i < size; i++) {
				data = session.receiveMessage();
				RemoteFile file = Utils.deserializeObject( data );
				files.add( file );
			}*/
			/*files = readGetAllResponse( remoteSession );
			
			//LOGGER.info( "Received all the files." );
		}
		catch( IOException e ) {
			if(!session.isClosed())
				session.close();
			closed = true;
		}
		
		if(remoteSession != null)
			remoteSession.close();
		
		closed = false;
		
		//session.close();
		
		return files;
	}*/
	
	@Override
	public DistributedFile get( String fileName ) throws DFSException
	{
		Preconditions.checkNotNull( fileName );
		
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		/*if(!fileName.startsWith( "./" ))
			fileName = "./" + fileName;
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		if(fileName.startsWith( database.getFileSystemRoot() ))
			fileName = fileName.substring( database.getFileSystemRoot().length() );*/
		fileName = fileName.replace( "\\", "/" );
		fileName = checkFile( fileName, database.getFileSystemRoot() );
		
		LOGGER.info( "starting GET operation: " + fileName );
		RemoteFile toWrite;
		DistributedFile backToClient;
		
		/*if(!testConnection()) {
			session = contactLoadBalancerNode();
			if(session == null) {
				LOGGER.info( "Sorry, but the service is down. Try again later." );
				return null;
			}
		}*/
		if(!contactLoadBalancerNode())
			return null;
		
		TCPSession remoteSession = null;
		
		try{
			// send the request
			//LOGGER.info( "Sending message..." );
			sendGetMessage( fileName );
			/*ByteBuffer msg = ByteBuffer.allocate( Byte.BYTES + Integer.BYTES + fileName.length() );
			msg.put( Utils.GET ).putInt( fileName.length() ).put( fileName.getBytes( StandardCharsets.UTF_8 ) );
			session.sendMessage( msg.array(), true );*/
			//LOGGER.info( "Message sent" );
			
			// checks whether the load balancer has founded an available node
			if(!checkResponse( session, "GET", false ))
				throw new IOException();
			
			//session.close();
			
			// check for remote connection and for quorum
			if((remoteSession = waitRemoteConnection( "GET" )) == null ||
			   !checkResponse( remoteSession, "GET", true ))
				throw new IOException();
			
			// receive one or more files
			/*byte[] data = session.receiveMessage();
			int size = Utils.byteArrayToInt( data );
			List<RemoteFile> files = new ArrayList<>( size );
			for(int i = 0; i < size; i++) {
				data = session.receiveMessage();
				files.add( Utils.deserializeObject( data ) );
			}*/
			List<RemoteFile> files = readGetResponse( remoteSession );
			
			//session.close();
			
			if(files.size() == 0) {
				LOGGER.info( "File \"" + fileName + "\" not found." );
				return null;
			}
			
			//LOGGER.info( "Received " + files.size() + " files." );
			
			int id = 0;
			if(files.size() > 1) {
				List<VectorClock> versions = new ArrayList<>();
				for(RemoteFile file : files)
					versions.add( file.getVersion() );
				
				id = makeReconciliation( fileName, versions );
				// Send back the reconciled version.
				if(!put( files.get( id ).getName() )) {
					throw new IOException();
				}
			}
			
			// Update the database.
			toWrite = files.get( id );
			if(database.saveFile( toWrite, toWrite.getVersion(), null, true ) != null)
				backToClient = new DistributedFile( toWrite, database.getFileSystemRoot() );
			else
				backToClient = getFile( fileName );
		}
		catch( IOException | SQLException | InterruptedException e ) {
			LOGGER.info( "Operation GET not performed. Try again later." );
			//e.printStackTrace();
			//if(session.isClosed())
				//closed = true;
			//if(!session.isClosed())
				//session.close();
			
			if(remoteSession != null)
				remoteSession.close();
			
			return null;
		}
		
		//session.close();
		LOGGER.info( "Operation GET succesfully completed. Received: " + backToClient );
		if(listener != null)
			listener.dbEvent( fileName, Message.GET );
		
		if(remoteSession != null)
			remoteSession.close();
		
		return backToClient;
	}
	
	/**
	 * Asks to the client which is the correct version.
	 * 
	 * @param fileName	the name of the file
	 * @param clocks	list of vector clocks
	 * 
	 * @return index of the selected file
	*/
	private synchronized int makeReconciliation( final String fileName, final List<VectorClock> clocks )
	{
		int size = clocks.size();
		if(size == 1)
			return 0;
		
		System.out.println( "There are multiple versions of the file '" + fileName + "', which are: " );
		for(int i = 0; i < size; i++)
			System.out.println( (i + 1) + ") " + clocks.get( i ) );
		
		while(true) {
			System.out.print( "Choose the correct version: " );
			int id = SCAN.nextInt() - 1;
			if(id >= 0 && id < size)
				return id;
			else
				System.out.println( "Error: select a number in the range [1-" + size + "]" );
		}
	}
	
	@Override
	public boolean put( String fileName ) throws DFSException, IOException
	{
		Preconditions.checkNotNull( fileName );
		
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		/*if(!fileName.startsWith( "./" ))
			fileName = "./" + fileName;
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		if(fileName.startsWith( database.getFileSystemRoot() ))
			fileName = fileName.substring( database.getFileSystemRoot().length() );*/
		fileName = fileName.replace( "\\", "/" );
		fileName = checkFile( fileName, database.getFileSystemRoot() );
		
		LOGGER.info( "starting PUT operation: " + fileName );
		
		DistributedFile file = database.getFile( fileName );
		System.out.println( "FILE: " + file );
		if(file == null || !Utils.existFile( database.getFileSystemRoot() + fileName, false )){
			if(database.checkExistFile( fileName )) {
				if(file == null)
					file = new DistributedFile( fileName, database.getFileSystemRoot(), new VectorClock() );
			}
			else {
				String error = "Operation PUT not performed: file \"" + fileName + "\" not founded. ";
				error += "The file must be present in one of the sub-directories of the root: " + database.getFileSystemRoot();
				LOGGER.error( error );
				return false;
			}
		}
		
		file.setDeleted( false );
		
		/*if(!testConnection()) {
			session = contactLoadBalancerNode();
			if(session == null) {
				LOGGER.info( "Sorry, but the service is down. Try again later." );
				return false;
			}
		}*/
		if(!contactLoadBalancerNode())
			return false;
		
		boolean completed = true;
		TCPSession remoteSession = null;
		
		try{
			//LOGGER.info( "Sending file..." );
			
			// send the file
			RemoteFile rFile = new RemoteFile( file, database.getFileSystemRoot() );
			sendPutMessage( rFile );
			/*byte[] msg = net.createMessage( new byte[]{ Utils.PUT },
											fileName.getBytes( StandardCharsets.UTF_8 ),
											true );
			msg = net.createMessage( msg, Utils.serializeObject( rFile ), true );
			session.sendMessage( msg, true );*/
			//LOGGER.info( "File sent" );
			
			// checks whether the load balancer has founded an available node
			if(!checkResponse( session, "PUT", false ))
				throw new IOException();
			
			//session.close();
			
			// checks whether the request has been forwarded to the storage node
			if((remoteSession = waitRemoteConnection( "PUT" )) == null ||
			   !checkResponse( remoteSession, "PUT", true ))
				throw new IOException();
			
			// Update its vector clock.
			// TODO ricevere il clock dallo storage node? forse si' cosi' almeno e' aggiornato correttamente
			//VectorClock newClock = Utils.deserializeObject( session.receiveMessage() );
			VectorClock newClock = rFile.getVersion().incremented( remoteSession.getSrcAddress() );
			database.saveFile( rFile, newClock, null, true );
		}
		catch( IOException | SQLException e ) {
			//e.printStackTrace();
			LOGGER.info( "Operation PUT not performed. Try again later." );
			completed = false;
			//if(session.isClosed())
				//closed = true;
			//session.close();
		}
		
		if(completed)
			LOGGER.info( "Operation PUT successfully completed." );
		
		if(remoteSession != null)
			remoteSession.close();
		
		//session.close();
		
		return completed;
	}
	
	@Override
	public boolean delete( String fileName ) throws IOException, DFSException
	{
		Preconditions.checkNotNull( fileName );
		
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		/*if(!fileName.startsWith( "./" ))
			fileName = "./" + fileName;
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		if(fileName.startsWith( database.getFileSystemRoot() ))
			fileName = fileName.substring( database.getFileSystemRoot().length() );*/
		fileName = fileName.replace( "\\", "/" );
		fileName = checkFile( fileName, database.getFileSystemRoot() );
		
		boolean completed = true;
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.getFile( fileId );
		if(file == null || !Utils.existFile( database.getFileSystemRoot() + fileName, false )) {
			LOGGER.error( "File \"" + fileName + "\" not founded." );
			return false;
		}
		
		if(file.isDirectory()) {
			for(File f: new File( database.getFileSystemRoot() + fileName ).listFiles()) {
				LOGGER.debug( "Name: " + f.getPath() + ", Directory: " + f.isDirectory() );
				completed |= delete( f.getPath() );
				if(!completed)
					break;
			}
		}
		
		LOGGER.info( "starting DELETE operation for: " + fileName );
		
		/*if(!testConnection()) {
			session = contactLoadBalancerNode();
			if(session == null) {
				LOGGER.info( "Sorry, but the service is down. Try again later." );
				return false;
			}
		}*/
		if(!contactLoadBalancerNode())
			return false;
		
		TCPSession remoteSession = null;
		
		try {
			/*byte[] msg = net.createMessage( new byte[]{ Utils.DELETE }, fileName.getBytes(), true );
			msg = net.createMessage( msg, Utils.serializeObject( file ), true );
			session.sendMessage( msg, true );*/
			sendDeleteMessage( file );
			
			// Checks whether the load balancer has founded an available node.
			if(!checkResponse( session, "DELETE", false ))
				throw new IOException();
			
			//session.close();
			
			// Check whether the request has been forwarded to the storage node.
			if((remoteSession = waitRemoteConnection( "DELETE" )) == null ||
			   !checkResponse( remoteSession, "DELETE", true ))
				throw new IOException();
			
			// Update its vector clock.
			// TODO ricevere il clock dallo storage node? forse si' cosi' almeno e' aggiornato correttamente
			//String nodeId = Utils.bytesToHex( Utils.getNodeId( 0, session.getSrcAddress() ).array() );
			//file.incrementVersion( remoteSession.getSrcAddress() );
			database.removeFile( fileName, file.getVersion().incremented( remoteSession.getSrcAddress() ), true );
		}
		catch( IOException | SQLException e ) {
			//e.printStackTrace();
			LOGGER.info( "Operation DELETE not performed. Try again later." );
			completed = false;
			//if(session.isClosed())
				//closed = true;
			//if(!session.isClosed())
				//session.close();
		}
		
		if(remoteSession != null)
			remoteSession.close();
		
		LOGGER.info( "DELETE operation for \"" + fileName + "\" completed successfully." );
		if(listener != null)
			listener.dbEvent( fileName, Message.DELETE );
		
		//session.close();
		
		return completed;
	}
	
	@Override
	public List<DistributedFile> listFiles()
	{
		return database.getAllFiles();
	}
	
	/**
	 * Tests whether the connection
	 * is still alive.
	*/
	private boolean testConnection()
	{
		if(session != null && !session.isClosed()) {
			try {
				session.setSoTimeout( 50 );
				session.receiveMessage();
				session.setSoTimeout( 0 );
			} catch( IOException e ) { return false; }
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * Try a connection with the first available load balancer node.
	 * 
	 * @return the remote connection if at least one remote node is available,
	 * 		   {@code null} otherwise.
	*/
	private boolean contactLoadBalancerNode()
	{
		if(testConnection())
			return true;
		
		LOGGER.info( "Contacting a remote node..." );
		
		session = null;
		HashSet<String> filterAddress = new HashSet<>();
		List<GossipMember> nodes = new ArrayList<>( loadBalancers );
		
		while(session == null) {
			if(filterAddress.size() == loadBalancers.size() || nodes.size() == 0)
				break;
			
			GossipMember partner = selectNode( nodes );
			if(filterAddress.contains( partner.getHost() ))
				nodes.remove( partner );
			else {
				filterAddress.add( partner.getHost() );
				System.out.println( "Contacting " + partner + "..." );
				try{ session = net.tryConnect( partner.getHost(), partner.getPort(), 2000 ); }
				catch( IOException e ) {
					//e.printStackTrace();
					nodes.remove( partner );
					System.out.println( "Node " + partner + " unreachable" );
				}
			}
		}
		
		if(session == null)
			LOGGER.error( "Sorry, but the service is not available. Retry later." );
		else {
			MessageRequest message = new MessageRequest( Message.HELLO );
			//message.setClientAddress( this.address + ":" + this.port );
			message.putMetadata( this.address + ":" + this.port, null );
			try {
				session.sendMessage( message, true );
				return true;
			} catch( IOException e ) {
				//e.printStackTrace();
			}
		}
		
		return false;
	}
	
	/**
	 * Wait the incoming connection from the StorageNode.
	 * 
	 * @param op	the requested operation
	 * 
	 * @return the connection if it has been established,
	 * 		   {@code null} otherwise
	*/
	private TCPSession waitRemoteConnection( final String op ) throws IOException
	{
		LOGGER.info( "Wait the incoming connection..." );
		TCPSession session = net.waitForConnection();
		if(session == null) {
			LOGGER.error( "Operation " + op.toUpperCase() + " not performed; try again later." );
		}
		
		return session;
	}
	
	/**
	 * Find a random node from the local membership list.
	 * The node is guaranteed to have been chosen uniformly.
	 *
	 * @return a random member
	 */
	private <T> T selectNode( final List<T> nodes )
	{
		int randomNeighborIndex = random.nextInt( nodes.size() );
		return nodes.get( randomNeighborIndex );
	}
	
	public static interface DBListener
	{
		public void dbEvent( final String fileName, final byte code );
	}
	
	/**
	 * Returns the state of the system
	 * 
	 * @return {@code true} if the system is down,
	 * 		   {@code false} otherwise.
	*/
	public boolean isClosed()
	{
		/*if(!closed && session != null && !session.isClosed()) {
			
			long currTime = System.currentTimeMillis();
			timer = timer - (currTime - timestamp);
			timestamp = currTime;
			
			if(!computing && timer <= 0) {
				try {
					session.sendMessage( new MessageRequest( Message.KEEP_ALIVE ), true );
					timer = KEEP_ALIVE;
				}
				catch( IOException e ){
					session.close();
					closed = true;
				}
			}
		}*/
		
		return closed;
	}
	
	public void shutDown()
	{
		//shutDown = true;
		//syncClient.stop();
		
		if(session != null && !session.isClosed()) {
			try{ session.sendMessage( new MessageRequest( Message.CLOSE ), true ); }
			catch( IOException e ) {}
			session.close();
		}
		net.close();
		
		if(!testing)
			database.shutdown();
		
		closed = true;
		
		LOGGER.info( "The service is closed." );
	}
	
	/**
	 * Class used to synchronize the client
	 * with a consistent view of
	 * the distributed file system.
	*/
	/*private class SynchronizeClient implements ActionListener
	{
		@Override
		public void actionPerformed( final ActionEvent e )
		{
			if(!shutDown) {
				syncClient.stop();
				
				List<RemoteFile> files = getAllFiles();
				if(files != null)
					checkFiles( files );
			}
			
			if(!shutDown)
				syncClient.restart();
		}
		
		/**
		 * The downloaded files are merged with the own ones.
		 * 
		 * @param files	
		*/
		/*private void checkFiles( final List<RemoteFile> files )
		{
			for(RemoteFile file : files) {
				DistributedFile myFile = database.getFile( Utils.getId( file.getName() ) );
				try {
					if(myFile == null ||
					   //!makeReconciliation( myFile.getName(), myFile.getVersion(), file.getVersion() )) {
					   makeReconciliation( myFile.getName(), Arrays.asList( myFile.getVersion(), file.getVersion() ) ) == 1) {
						if(!file.isDeleted())
							database.saveFile( file, file.getVersion(), null, true );
						else
							database.removeFile( file.getName(), file.getVersion(), true );
					}
				}
				catch( IOException | SQLException e ) {}
			}
		}
		
		/*private boolean makeReconciliation( final String fileName, final VectorClock myClock, final VectorClock vClock )
		{
			List<Versioned<Integer>> versions = new ArrayList<>();
			versions.add( new Versioned<Integer>( 0, myClock ) );
			versions.add( new Versioned<Integer>( 1, vClock ) );
			
			// get the list of concurrent versions
			VectorClockInconsistencyResolver<Integer> vecResolver = new VectorClockInconsistencyResolver<>();
			List<Versioned<Integer>> inconsistency = vecResolver.resolveConflicts( versions );
			
			// resolve the conflicts, using a time-based resolver
			TimeBasedInconsistencyResolver<Integer> resolver = new TimeBasedInconsistencyResolver<>();
			int id = resolver.resolveConflicts( inconsistency ).get( 0 ).getValue();
			
			return id == 0;
		}*/
	//}
}