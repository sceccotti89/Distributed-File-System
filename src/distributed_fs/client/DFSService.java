/**
 * @author Stefano Ceccotti
*/

package distributed_fs.client;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import javax.swing.Timer;

import org.json.JSONException;

import distributed_fs.exception.DFSException;
import distributed_fs.files.DFSDatabase;
import distributed_fs.files.DistributedFile;
import distributed_fs.files.RemoteFile;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.messages.Message;
import distributed_fs.net.messages.MessageRequest;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.TimeBasedInconsistencyResolver;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.VectorClockInconsistencyResolver;
import distributed_fs.versioning.Versioned;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class DFSService extends DFSManager implements IDFSService
{
	private boolean initialized = false;
	private boolean shutDown = false;
	
	private final Random random;
	
	private final TCPnet net;
	private final DFSDatabase database;
	private Timer syncClient;
	
	private boolean testing = false;
	
	private static final Scanner SCAN = new Scanner( System.in );
	
	/** Time to wait before to check the database (20 seconds). */
	private static final int CHECK_TIMER = 20000;
	
	public DFSService() throws IOException, JSONException
	{
		super();
		
		net = new TCPnet( _address, Utils.SERVICE_PORT );
		net.setSoTimeout( 5000 );
		
		Utils.createDirectory( Utils.RESOURCE_LOCATION );
		
		random = new Random();
		database = new DFSDatabase( null );
		
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
	public DFSService( final String address, final List<GossipMember> members ) throws IOException, JSONException
	{
		//super();
		
		testing = true;
		this._address = address;
		
		net = new TCPnet( _address, Utils.SERVICE_PORT );
		net.setSoTimeout( 5000 );
		
		Utils.createDirectory( Utils.RESOURCE_LOCATION );
		
		random = new Random();
		database = new DFSDatabase( null );
		
		loadBalancers.clear();
		for(GossipMember member : members) {
			if(member.getNodeType() == GossipMember.LOAD_BALANCER)
				loadBalancers.add( new RemoteGossipMember( member.getHost(), member.getPort(), "", 0, GossipMember.LOAD_BALANCER ) );
		}
	}
	
	/**
	 * Starts the service.
	 * 
	 * @return {@code true} if the service has been started,
	 * 		   {@code false} otherwise.
	*/
	public boolean start()
	{
		SynchronizeClient client = new SynchronizeClient();
		syncClient = new Timer( CHECK_TIMER, client );
		
		List<RemoteFile> files = getAllFiles();
		if(files == null) {
			LOGGER.info( "The system is not available. Retry later." );
			return false;
		}
		client.checkFiles( files );
		//TODO syncClient.start();
		
		LOGGER.info( "System up." );
		return (initialized = true);
	}
	
	/**
	 * Returns the requested file, from the internal database.
	 * 
	 * @return the file, if present, {@code null} otherwise
	*/
	public DistributedFile getFile( String fileName ) {
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName = fileName + "/";
		
		DistributedFile file = database.getFile( Utils.getId( fileName ) );
		if(file == null || file.isDeleted())
			return null;
		
		return file;
	}
	
	/**
	 * Retrieves all the files stored in a random node of the network.
	*/
	private List<RemoteFile> getAllFiles()
	{
		LOGGER.info( "Synchonizing..." );
		
		List<RemoteFile> files = null;
		
		if(session == null || session.isClosed()) {
			session = contactLoadBalancerNode( false );
			if(session == null)
				return null;
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
			files = readGetAllResponse( remoteSession );
			
			//LOGGER.info( "Received all the files." );
		}
		catch( IOException e ) {
			if(session.isClosed())
				session.close();
		}
		
		if(remoteSession != null)
			remoteSession.close();
		
		//session.close();
		
		return files;
	}
	
	@Override
	public DistributedFile get( String fileName ) throws DFSException
	{
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		
		LOGGER.info( "starting GET operation: " + fileName );
		RemoteFile toWrite;
		DistributedFile backToClient;
		
		if(session == null || session.isClosed()) {
			session = contactLoadBalancerNode( false );
			if(session == null)
				return null;
		}
		
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
				LOGGER.info( "File " + fileName + " not found." );
				return null;
			}
			
			//LOGGER.info( "Received " + files.size() + " files." );
			
			int id = 0;
			if(files.size() > 1) {
				id = makeReconciliation( files );
				// send back the reconciled version
				if(!put( files.get( id ).getName() )) {
					throw new IOException();
				}
			}
			
			// update the database
			toWrite = files.get( id );
			if(!database.saveFile( toWrite, null, true ))
				backToClient = getFile( fileName );
			else
				backToClient = new DistributedFile( toWrite );
		}
		catch( IOException | SQLException e ) {
			LOGGER.info( "Operation GET not performed. Try again later." );
			//e.printStackTrace();
			//if(session != null) session.close();
			if(session.isClosed())
				session.close();
			
			if(remoteSession != null)
				remoteSession.close();
			
			return null;
		}
		
		//session.close();
		LOGGER.info( "Operation GET succesfully completed." );
		
		if(remoteSession != null)
			remoteSession.close();
		
		return backToClient;
	}
	
	/**
	 * Asks to the client which is the correct version.
	 * 
	 * @param files		list of files to merge
	 * 
	 * @return index of the selected file
	*/
	private int makeReconciliation( final List<RemoteFile> files ) // TODO metterci una lista di clock? cosi' puo' essere richiamato anche da sotto
	{
		int size = files.size();
		if(size == 1)
			return 0;
		
		System.out.println( "There are multiple versions of the file:" );
		for(int i = 0; i < size; i++)
			System.out.println( (i + 1) + ") " + files.get( i ).getVersion() );
		
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
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		
		LOGGER.info( "starting PUT operation: " + fileName );
		
		DistributedFile file = database.getFile( Utils.getId( fileName ) );
		if(file == null || !Utils.existFile( fileName, false )) {
			LOGGER.error( "Operation PUT not performed: file \"" + fileName + "\" not founded." );
			return false;
		}
		file.setDeleted( false );
		
		if(session == null || session.isClosed()) {
			session = contactLoadBalancerNode( false );
			if(session == null)
				return false;
		}
		
		boolean completed = true;
		TCPSession remoteSession = null;
		
		try{
			//LOGGER.info( "Sending file..." );
			
			// send the file
			RemoteFile rFile = new RemoteFile( file );
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
			
			// update its vector clock
			//String nodeId = Utils.bytesToHex( Utils.getNodeId( 0, session.getSrcAddress() ).array() );
			rFile.incrementVersion( remoteSession.getSrcAddress() );
			database.saveFile( rFile, null, false );
		}
		catch( IOException | SQLException e ) {
			LOGGER.info( "Operation PUT not performed. Try again later." );
			completed = false;
			if(session.isClosed())
				session.close();
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
		if(!initialized)
			throw new DFSException( "The system has not been initialized." );
		
		if(Utils.isDirectory( fileName ) && !fileName.endsWith( "/" ))
			fileName += "/";
		
		LOGGER.info( "starting DELETE operation for: " + fileName );
		
		boolean completed = true;
		
		ByteBuffer fileId = Utils.getId( fileName );
		DistributedFile file = database.getFile( fileId );
		if(file == null || !Utils.existFile( fileName, false )) {
			LOGGER.error( "File \"" + fileName + "\" not founded." );
			return false;
		}
		
		if(file.isDirectory()) {
			for(File f: new File( fileName ).listFiles()) {
				LOGGER.debug( "Name: " + f.getPath() + ", Directory: " + f.isDirectory() );
				completed |= delete( f.getPath() );
				if(!completed)
					break;
			}
		}
		
		if(session == null || session.isClosed()) {
			session = contactLoadBalancerNode( true );
			if(session == null)
				return false;
		}
		
		TCPSession remoteSession = null;
		
		try {
			// send the request
			/*byte[] msg = net.createMessage( new byte[]{ Utils.DELETE }, fileName.getBytes(), true );
			msg = net.createMessage( msg, Utils.serializeObject( file ), true );
			session.sendMessage( msg, true );*/
			sendDeleteMessage( file );
			
			// checks whether the load balancer has founded an available node
			if(!checkResponse( session, "DELETE", false ))
				throw new IOException();
			
			//session.close();
			
			// check whether the request has been forwarded to the storage node
			if((remoteSession = waitRemoteConnection( "DELETE" )) == null ||
			   !checkResponse( remoteSession, "DELETE", true ))
				throw new IOException();
			
			// update its vector clock
			//String nodeId = Utils.bytesToHex( Utils.getNodeId( 0, session.getSrcAddress() ).array() );
			//file.incrementVersion( remoteSession.getSrcAddress() );
			database.removeFile( fileName, file.getVersion().incremented( remoteSession.getSrcAddress() ) );
		}
		catch( IOException | SQLException e ) {
			LOGGER.info( "Operation DELETE not performed. Try again later." );
			completed = false;
			if(session.isClosed())
				session.close();
		}
		
		if(remoteSession != null)
			remoteSession.close();
		
		LOGGER.info( "DELETE operation for \"" + fileName + "\" completed successfully." );
		
		//session.close();
		
		return completed;
	}
	
	/**
	 * Try a connection with the first available load balancer node.
	 * 
	 * @return the remote connection if at least one remote node is available,
	 * 		   {@code null} otherwise.
	*/
	private TCPSession contactLoadBalancerNode( final boolean persistent )
	{
		LOGGER.info( "Contacting a remote node..." );
		
		TCPSession session = null;
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
					e.printStackTrace();
					nodes.remove( partner );
					System.out.println( "Node " + partner + " unreachable" );
				}
			}
		}
		
		if(session == null)
			LOGGER.error( "The service is not available. Retry later." );
		
		return session;
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
	
	public void shutDown()
	{
		shutDown = true;
		syncClient.stop();
		
		if(session != null && !session.isClosed()) {
			try{ session.sendMessage( new MessageRequest( Message.CLOSE ), true ); }
			catch( IOException e ) {}
			session.close();
		}
		
		net.close();
		if(!testing) database.shutdown();
		
		LOGGER.info( "The service is closed." );
	}
	
	/**
	 * Class used to synchronize the client
	 * with a consistent view of
	 * the distributed file system.
	*/
	private class SynchronizeClient implements ActionListener
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
		private void checkFiles( final List<RemoteFile> files )
		{
			for(RemoteFile file : files) {
				DistributedFile myFile = database.getFile( Utils.getId( file.getName() ) );
				try {
					if(myFile == null ||
							!makeReconciliation( myFile.getVersion(), file.getVersion() )) {
						if(!file.isDeleted())
							database.saveFile( file, null, true );
						else
							database.removeFile( file.getName(), file.getVersion() );
					}
				}
				catch( IOException | SQLException e ) {}
			}
		}
		
		private boolean makeReconciliation( final VectorClock myClock, final VectorClock vClock )
		{
			// TODO usare questa o quella da utente??
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
		}
	}
}