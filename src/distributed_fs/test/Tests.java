/**
 * @author Stefano Ceccotti
*/

package distributed_fs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import distributed_fs.anti_entropy.AntiEntropySenderThread;
import distributed_fs.client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class Tests
{
	private static final String myIpAddress = "127.0.0.1";
	
	private List<DFSService> clients;
	private List<GossipMember> members;
	private List<DFSNode> nodes;
	
	private static final int NUMBER_OF_BALANCERS = 2;
	private static final int NUMBER_OF_STORAGES = 5;
	
	public static void main( final String[] args ) throws Exception
	{
	    //8002 = 2CCDB0A66E41511C0BC786727EB2A254CCCD39BB
	    //8105 = CF84B271DD59C6F89A106BBA2FE7063D1C17E00B
	    //8210 = E2352D531FAB14375362844A9C4516601359F49D
	    //8317 = 496D95264EC975412E55AC8EBA12F93B7AA834DF
	    //8426 = AA3592473714D61449399A9D2FD9F583D74F7ADE
	    
	    //BasicConfigurator.configure();
	    //DFSDatabase db = new DFSDatabase( "./Servers/Resources6/", "./Servers/Database6/", null );
	    /*DistributedFile file = db.getFile( "chord_sigcomm.pdf" );
	    System.out.println( "FILE: " + file );
	    db.saveFile( file.getName(), null, file.getVersion().incremented( "pipp" ), "pippo franco", false );
	    file = db.getFile( "chord_sigcomm.pdf" );
	    System.out.println( "FILE: " + file );*/
	    //System.out.println( "FILES: " + db.getKeysInRange( "AA3592473714D61449399A9D2FD9F583D74F7ADE", "496D95264EC975412E55AC8EBA12F93B7AA834DF" ) );
	    
	    //db.close();
        
	    new Tests();
	}
	
	public Tests() throws Exception
	{
	    DFSUtils.deleteDirectory( new File( "Servers" ) );
	    DFSUtils.deleteDirectory( new File( "Clients" ) );
	    
	    System.out.println( "Start tests..." );
		//testDatabase();
		
		// First test when the service is down.
		//DFSService service = new DFSService( myIpAddress, 0, false, null, null, null, null );
		//assertFalse( service.start() );
		//service.shutDown();
		
		startSystem( false );
		
		//Thread.sleep( 2000 );
		
		//DFSService service = new DFSService( myIpAddress, 9002, true, members, null, null, null );
        //service.start();
		
		//testNoLoadBalancers( myIpAddress );
		testSingleClient();
		stressTest();
		testAntiEntropy();
		testHintedHandoff();
		
		close();
		
		System.out.println( "End of tests." );
	}
	
	@Before
	public void startSystem( final boolean disableAntiEntropy ) throws IOException, JSONException, SQLException, InterruptedException, DFSException
	{
		runServers( disableAntiEntropy );
		runClients( myIpAddress, 2 );
	}
	
	@Before
	private void runServers( final boolean disableAntiEntropy )
	        throws IOException, JSONException, SQLException, InterruptedException, DFSException
	{
		// Create the gossip members and put them in a list.
		members = new ArrayList<>( NUMBER_OF_BALANCERS + NUMBER_OF_STORAGES );
		
		int k = 100;
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
			int port = 8000 + (i * k);
			String id = DFSUtils.getNodeId( 1, myIpAddress + ":" + port );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( myIpAddress, port, id, 1, GossipMember.LOAD_BALANCER ) );
		}
		for(int i = 0 ; i < NUMBER_OF_STORAGES; i++, k++) {
			int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
			//System.out.println( "[" + i + "] = " + port );
			String id = DFSUtils.getNodeId( 1, myIpAddress + ":" + port );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( myIpAddress, port, id, 1, GossipMember.STORAGE ) );
		}
		
		nodes = new ArrayList<>( members.size() );
		
		// Start the load balancer nodes.
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
			LoadBalancer node = new LoadBalancer( members, members.get( i ).getPort(), myIpAddress );
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (JSONException e) {e.printStackTrace();} }
			}.start();
		}
		
		String resources = "./Servers/Resources";
		String database =  "./Servers/Database";
		// Start the storage nodes.
		for(int i = 0; i < NUMBER_OF_STORAGES; i++) {
			GossipMember member = members.get( i + NUMBER_OF_BALANCERS );
			System.out.println( "Port: " + member.getPort() + ", Resources: " + "./Resources" + (i+2) + "/" );
			StorageNode node = new StorageNode( members, member.getId(), i + 2, myIpAddress, member.getPort(),
												resources + (i+2) + "/", database + (i+2) + "/" );
			if(disableAntiEntropy)
			    node.disableAntiEntropy();
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (JSONException e) {e.printStackTrace();} }
			}.start();
		}
		
		Thread.sleep( 100 );
	}
	
	@Before
	private void runClients( final String ipAddress, final int numClients ) throws IOException, JSONException, DFSException
	{
		int port = 9000;
		clients = new ArrayList<>( numClients );
		for(int i = 0; i < numClients; i++) {
			DFSService service = new DFSService( ipAddress, port + i, true, members,
			                                     "./Clients/ResourcesClient" + (i + 1),
			                                     "./Clients/DatabaseClient" + (i + 1), null );
			service.disableSyncThread();
			clients.add( service );
			assertTrue( service.start() );
		}
	}
	
	@Test
	public void testNoLoadBalancers( final String ipAddress ) throws IOException, JSONException, DFSException, InterruptedException
	{
	    DFSService service = new DFSService( ipAddress, 9002, false, members, null, null, null );
	    service.start();
	    
	    Thread.sleep( 2000 );
	    service.get( "test2.txt" );
	    service.get( "chord_sigcomm.pdf" );
	    service.delete( "test2.txt" );
	    service.put( "chord_sigcomm.pdf" );
	    
	    service.shutDown();
	}
	
	@Test
	public void testSingleClient() throws IOException, JSONException, DFSException, InterruptedException
	{
		DFSService service = clients.get( 0 );
		
		DFSUtils.existFile( "./Clients/ResourcesClient1/Images/photoshop.pdf", true );
		
		String file = "";
		assertEquals( service.get( file ), service.getFile( file ) );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		file = "./Clients/ResourcesClient1/Images/photoshop.pdf";
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
    public void stressTest() throws InterruptedException
    {
    	List<Thread> clientThreads = new ArrayList<>( clients.size() );
    	
    	for(int i = 0; i < clients.size(); i++) {
    		final int index = i;
    		Thread t = new Thread() {
    			@Override
    			public void run() { try { testMultipleClient( index ); } catch (Exception e) {e.printStackTrace();} }
    		};
    		t.start();
    		clientThreads.add( t );
    	}
    	
    	// Wait the termination of all the clients.
    	for(Thread t : clientThreads)
    		t.join();
    }
	
    @Test
	public void testAntiEntropy() throws IOException, JSONException, DFSException, InterruptedException, SQLException
	{
		System.out.println( "\n\nTEST ANTI ENTROPY" );
		
		String file = "test3.txt";
		DFSUtils.existFile( "./Clients/ResourcesClient1/" + file, true );
		
		modifyTextFile( "./Clients/ResourcesClient1/" + file );
		// TODO a volte sembra si blocchi in questo punto.
		// TODO indagare un po' sulle cause.
		assertTrue( clients.get( 0 ).put( file ) );
		//assertTrue( clients.get( 0 ).put( "test4.txt" ) );
		
		System.out.println( "Waiting..." );
		// Wait the necessary time before to check if the last node have received the file.
		Thread.sleep( AntiEntropySenderThread.EXCH_TIMER * 2 + 1000 );
		System.out.println( "NODE: " + members.get( 0 + NUMBER_OF_BALANCERS ) +
							", FILE: " + nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
		
		readFile( "./Servers/Resources6/" + file );
		assertNotNull( nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
		
		/*assertTrue( clients.get( 0 ).delete( file ) );
		//assertTrue( clients.get( 0 ).delete( "test4.txt" ) );
		System.out.println( "Waiting..." );
        // Wait the necessary time before to check if the last node have received the updated file.
        Thread.sleep( AntiEntropySenderThread.EXCH_TIMER * 2 + 1000 );
        System.out.println( "NODE: " + members.get( 0 + NUMBER_OF_BALANCERS ) +
                            ", FILE: " + nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
        assertNull( nodes.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );*/
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
    public void testHintedHandoff() throws IOException, JSONException, DFSException, InterruptedException
    {
		System.out.println( "Starting Hinted Handoff test..." );
    	String file = "chord_sigcomm.pdf";
    	DFSUtils.existFile( "./Clients/ResourcesClient1/" + file, true );
    	
    	int index = 3 + NUMBER_OF_BALANCERS;
    	String hh = nodes.get( index ).getAddress() + ":" + nodes.get( index ).getPort();
    	nodes.get( index ).closeResources();
    	System.out.println( "Node: " + members.get( index ) + " closed." );
    	
    	Thread.sleep( 2000 );
    	
    	assertTrue( clients.get( 0 ).put( file ) );
    	System.out.println( "\n\n" );
    	Thread.sleep( 2000 );
    	
    	//assertEquals( service.get( file ), service.getFile( file ) );
    	System.out.println( "HH: " + nodes.get( 4 + NUMBER_OF_BALANCERS ).getFile( file ) );
    	assertEquals( nodes.get( 4 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff(), hh );
    }
	
    @Test
	private void testMultipleClient( final int index ) throws IOException, DFSException, InterruptedException
	{
		DFSService service = clients.get( index );
		
		DFSUtils.existFile( "./Clients/ResourcesClient" + (index + 1) + "/Images/photoshop.pdf", true );
		
		service.get( "" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.put( "Images/photoshop.pdf" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		service.get( "Test3968.pdf" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.get( "unknown.pdf" );
		System.out.println( "\n\n" );
		
		//Thread.sleep( 1000 );
		
		service.put( "Test2.txt" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		DFSUtils.existFile( "./Clients/ResourcesClient" + (index + 1) + "/test2.txt", true );
		service.put( "test2.txt" );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
	}
    
	@Test
	public void testDatabase() throws IOException, DFSException, SQLException, InterruptedException
	{
		if(clients == null && nodes == null)
			BasicConfigurator.configure();
		
		DFSDatabase database = new DFSDatabase( null, null, null );
		DistributedFile file;
		
		for(int i = 1; i <= 10; i++)
			assertNotNull( database.saveFile( "Test" + i + ".txt", null, new VectorClock(), null, false ) );
		assertNull( database.getFile( "Test0.txt" ) );
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		//file = new DistributedFile( file.read() );
		file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
		file.incrementVersion( "pippo" );
		assertNotNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
		
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		//file = new DistributedFile( file.read() );
		file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
		file.setVersion( new VectorClock() );
		assertNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
		
		assertNull( database.getFile( "" ) );
		assertNull( database.getFile( "Test11.txt" ) );
		
		assertNull( database.removeFile( file.getName(), file.getVersion(), null ) );
		assertNotNull( database.removeFile( file.getName(), new VectorClock().incremented( "pippo" ).incremented( "pippo" ), null ) );
		assertTrue( database.getFile( file.getName() ).isDeleted() );
		
		database.close();
		
		if(clients == null && nodes == null)
			BasicConfigurator.resetConfiguration();
	}
	
	@After
	public void close() throws InterruptedException
	{
	    if(clients != null) {
    		for(DFSService service : clients)
    			service.shutDown();
	    }
	    
	    if(nodes != null) {
    		for(DFSNode node : nodes)
    			node.closeResources();
    		for(DFSNode node : nodes)
    			node.join();
	    }
	}
}