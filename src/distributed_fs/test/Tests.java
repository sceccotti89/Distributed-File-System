/**
 * @author Stefano Ceccotti
*/

package distributed_fs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import distributed_fs.anti_entropy.AntiEntropySenderThread;
import distributed_fs.client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.files.DFSDatabase;
import distributed_fs.files.DistributedFile;
import distributed_fs.overlay.DFSnode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class Tests
{
	private static String myIpAddress;
	
	private List<DFSService> services;
	private List<GossipMember> members;
	private List<DFSnode> nodes;
	
	private static final int NUMBER_OF_BALANCERS = 2;
	private static final int NUMBER_OF_NODES = 5;
	
	public static void main( String[] args ) throws Exception
	{
		new Tests();
	}
	
	public Tests() throws Exception
	{
		testDatabase();
		
		// First test when the service is down.
		DFSService service = new DFSService( myIpAddress, 0, null, null, null, null );
		assertFalse( service.start() );
		service.shutDown();
		
		startSystem();
		
		Thread.sleep( 2000 );
		
		testSingleClientOperations();
		stressTest();
		testAntiEntropy();
		testHintedHandoff();
		
		close();
	}
	
	@Before
	public void startSystem() throws IOException, JSONException, SQLException, InterruptedException, DFSException
	{
		// Enumerate all the network intefaces.
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netInt : Collections.list( nets )) {
			if(netInt.getName().equals( "eth0" )) {
				// Enumerate all the IP address associated with it.
				for(InetAddress inetAddress : Collections.list( netInt.getInetAddresses() )) {
					if(!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
						myIpAddress = inetAddress.getHostAddress();
						break;
					}
				}
			}
			
			if(myIpAddress != null)
				break;
		}
		
		runServers();
		runClients( myIpAddress, 2 );
	}
	
	@Before
	private void runServers() throws IOException, JSONException, SQLException, InterruptedException, DFSException
	{
		// Create the gossip members and put them in a list.
		members = new ArrayList<>();
		
		int k = 100;
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
			int port = 8000 + (i * k);
			String id = Utils.bytesToHex( Utils.getNodeId( 1, myIpAddress + ":" + port ).array() );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( myIpAddress, port, id, 1, GossipMember.LOAD_BALANCER ) );
		}
		for(int i = 0 ; i < NUMBER_OF_NODES; i++, k++) {
			int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
			String id = Utils.bytesToHex( Utils.getNodeId( 1, myIpAddress + ":" + port ).array() );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( myIpAddress, port, id, 1, GossipMember.STORAGE ) );
		}
		
		nodes = new ArrayList<>();
		
		// Start the load balancer nodes.
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
			LoadBalancer node = new LoadBalancer( members, members.get( i ).getPort(), myIpAddress );
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (JSONException e) {e.printStackTrace();} }
			}.start();
		}
		
		// Start the storage nodes.
		for(int i = 0; i < NUMBER_OF_NODES; i++) {
			GossipMember member = members.get( i + NUMBER_OF_BALANCERS );
			System.out.println( "Port: " + member.getPort() + ", Resources: " + "./Resources" + (i+2) + "/" );
			StorageNode node = new StorageNode( members, member.getId(), myIpAddress, member.getPort(),
												"./Resources" + (i+2) + "/", "./Database" + (i+2) + "/DFSDatabase" );
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (JSONException e) {e.printStackTrace();} }
			}.start();
		}
	}
	
	@Before
	private void runClients( final String address, final int numClients ) throws IOException, JSONException, DFSException
	{
		int port = 9000;
		services = new ArrayList<>( numClients );
		for(int i = 0; i < numClients; i++) {
			DFSService service = new DFSService( address, port + i, members, null, null, null );
			services.add( service );
			assertTrue( service.start() );
		}
	}
	
	@Test
	public void testSingleClientOperations() throws IOException, JSONException, DFSException, InterruptedException
	{
		DFSService service = services.get( 0 );
		
		Utils.existFile( "./Resources/Images/photoshop.pdf", true );
		
		String file = "";
		assertEquals( service.get( file ), service.getFile( file ) );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		file = "./Resources/Images/photoshop.pdf";
		assertEquals( service.put( file ), true );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.get( file ), service.getFile( file ) );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		assertEquals( service.get( file ), service.getFile( file ) );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		assertEquals( service.put( file ), true );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.put( "" ), false );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( "" ), false );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( file ), true );
		assertEquals( service.getFile( file ), null );
	}
	
	@Test
	public void testHintedHandoff() throws IOException, JSONException, DFSException, InterruptedException
	{
		String file = "chord_sigcomm.pdf";
		int index = 4 + NUMBER_OF_BALANCERS;
		String hh = nodes.get( index ).getAddress() + ":" + nodes.get( index ).getPort();
		nodes.get( index ).closeResources();
		System.out.println( "Node: " + members.get( index ) + " closed." );
		
		Thread.sleep( 2000 );
		
		assertTrue( services.get( 0 ).put( file ) );
		System.out.println( "\n\n" );
		Thread.sleep( 2000 );
		
		//assertEquals( service.get( file ), service.getFile( file ) );
		//System.out.println( "HH: " + nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
		assertEquals( nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff(), hh );
	}
	
	@Test
	public void testAntiEntropy() throws IOException, JSONException, DFSException, InterruptedException, SQLException
	{
		//System.out.println( "SUCCESSOR: " + nodes.get( 0 + NUMBER_OF_BALANCERS ).getSuccessor() );
		System.out.println( "\n\nTEST ANTI ENTROPY" );
		
		String file = "test3.txt";
		
		modifyTextFile( "./Resources/" + file );
		assertTrue( services.get( 0 ).put( file ) );
		
		System.out.println( "Waiting..." );
		// Wait the necessary time before to check if the last node have received the file.
		Thread.sleep( AntiEntropySenderThread.EXCH_TIMER * 2 + 500 );
		System.out.println( "NODE: " + members.get( 4 + NUMBER_OF_BALANCERS ) +
							", FILE: " + nodes.get( 4 + NUMBER_OF_BALANCERS ).getFile( file ) );
		
		readFile( "./Resources6/" + file );
	}
	
	private void readFile( final String file ) throws IOException
	{
		BufferedReader br = new BufferedReader( new FileReader( file ) );
		System.out.println( "Line: " + br.readLine() );
		br.close();
	}
	
	private void modifyTextFile( final String file ) throws IOException
	{
		PrintWriter writer = new PrintWriter( file, StandardCharsets.UTF_8.name() );
		writer.println( new Date().toString() );
		writer.close();
	}
	
	@Test
	public void stressTest() throws InterruptedException
	{
		List<Thread> clients = new ArrayList<>( services.size() );
		
		for(int i = 0; i < services.size(); i++) {
			final int index = i;
			Thread t = new Thread() {
				@Override
				public void run() { try { testMultipleClientOperations( index ); } catch (Exception e) {e.printStackTrace();} }
			};
			t.start();
			clients.add( t );
		}
		
		// Wait the termination of all the clients.
		for(Thread t : clients)
			t.join();
	}
	
	@Test
	private void testMultipleClientOperations( final int index ) throws IOException, DFSException, InterruptedException
	{
		DFSService service = services.get( index );
		
		Utils.existFile( "./Resources/Images/photoshop.pdf", true );
		
		service.get( "" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.put( "./Resources/Images/photoshop.pdf" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		service.get( "./Resources/Test3968.pdf" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.get( "./Resources/unknown.pdf" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.put( "./Resources/Test2.txt" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		service.put( "./Resources/test2.txt" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
	}

	@Test
	public void testDatabase() throws IOException, DFSException, SQLException
	{
		if(services == null && nodes == null)
			BasicConfigurator.configure();
		
		DFSDatabase database = new DFSDatabase( null, null, null );
		DistributedFile file;
		
		for(int i = 1; i <= 10; i++)
			assertNotNull( database.saveFile( "Test" + i + ".txt", null, new VectorClock(), null, false ) );
		assertNull( database.getFile( "Test0.txt" ) );
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		file = new DistributedFile( file.read() );
		file.getVersion().incrementVersion( "pippo" );
		assertNotNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
		
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		file = new DistributedFile( file.read() );
		file.setVersion( new VectorClock() );
		assertNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
		
		assertNull( database.getFile( "" ) );
		assertNull( database.getFile( "Test11.txt" ) );
		
		assertNull( database.removeFile( file.getName(), file.getVersion(), true ) );
		assertNotNull( database.removeFile( file.getName(), new VectorClock().incremented( "pippo" ).incremented( "pippo" ), true ) );
		assertTrue( database.getFile( file.getName() ).isDeleted() );
		
		database.shutdown();
	}

	@After
	public void close() throws InterruptedException
	{
		for(DFSService service : services)
			service.shutDown();
		
		for(DFSnode node : nodes)
			node.closeResources();
		for(DFSnode node : nodes)
			node.join();
	}
}