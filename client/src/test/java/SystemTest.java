
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import client.DFSService;
import distributed_fs.overlay.manager.anti_entropy.AntiEntropyThread;
import distributed_fs.exception.DFSException;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class SystemTest
{
    private static final String IpAddress = "127.0.0.1";
    private static final boolean enableAntiEntropy = true;
    
    private List<DFSService> clients;
    private List<GossipMember> members;
    private List<DFSNode> servers;
    
    private static final int NUMBER_OF_BALANCERS = 2;
    private static final int NUMBER_OF_STORAGES = 5;
    private static final int CLIENTS = 2;
    
    
    
    @Before
    public void setup() throws IOException, DFSException, InterruptedException
    {
        DFSUtils.testing = true;
        System.out.println( "Start tests..." );
        
        DFSUtils.deleteDirectory( new File( "Servers" ) );
        DFSUtils.deleteDirectory( new File( "Clients" ) );
        
        runServers();
        runClients( IpAddress, CLIENTS );
    }
    
    private void runServers() throws IOException, DFSException, InterruptedException
    {
        // Create the gossip members and put them in a list.
		members = new ArrayList<>( NUMBER_OF_BALANCERS + NUMBER_OF_STORAGES );
		
		int k = 100;
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
			int port = 8000 + (i * k);
			String id = DFSUtils.getNodeId( 1, IpAddress + ":" + port );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( IpAddress, port, id, 1, GossipMember.LOAD_BALANCER ) );
		}
		
		for(int i = 0 ; i < NUMBER_OF_STORAGES; i++, k++) {
			int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
			//System.out.println( "[" + i + "] = " + port );
			String id = DFSUtils.getNodeId( 1, IpAddress + ":" + port );
			//System.out.println( "ID: " + id );
			members.add( new RemoteGossipMember( IpAddress, port, id, 1, GossipMember.STORAGE ) );
		}
		
		servers = new ArrayList<>( members.size() );
		
		// Start the load balancer nodes.
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
			LoadBalancer node = new LoadBalancer( IpAddress, members.get( i ).getPort(), members );
			node.setGossipingMechanism( false );
			servers.add( node );
			node.launch( true );
		}
		
		String resources = "./Servers/Resources";
		String database =  "./Servers/Database";
		// Start the storage nodes.
		for(int i = 0; i < NUMBER_OF_STORAGES; i++) {
			GossipMember member = members.get( i + NUMBER_OF_BALANCERS );
			//System.out.println( "Port: " + member.getPort() + ", Resources: " + resources + (i+2) + "/" );
			StorageNode node = new StorageNode( IpAddress, member.getPort(), 1, members,
												resources + (i+2) + "/", database + (i+2) + "/" );
			node.setAntiEntropy( enableAntiEntropy );
			node.setGossipingMechanism( false );
			servers.add( node );
			node.launch( true );
		}
    }
    
    private void runClients( final String ipAddress, final int numClients ) throws IOException, DFSException
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
    public void startTests() throws IOException, DFSException, InterruptedException
    {
        testNoLoadBalancers();
        testSingleClient();
        testDeleteFolder();
        stressTest();
        testAntiEntropy();
        testHintedHandoff();
    }
    
    private void testNoLoadBalancers() throws IOException, DFSException, InterruptedException
	{
	    DFSService service = new DFSService( IpAddress, 9002, false, members, "./Clients/ResourcesClient/", "./Clients/DatabaseClient/", null );
	    service.disableSyncThread();
	    service.start();
	    
	    Thread.sleep( 2000 );
	    DFSUtils.existFile( "./Clients/ResourcesClient/test.txt", true );
	    assertTrue( service.put( "test.txt" ) );
	    assertNotNull( service.get( "test.txt" ) );
	    DFSUtils.existFile( "./Clients/ResourcesClient/chord_sigcomm2.pdf", true );
	    assertTrue( service.put( "chord_sigcomm2.pdf" ) );
	    assertTrue( service.delete( "test.txt" ) );
	    assertTrue( service.delete( "chord_sigcomm2.pdf" ) );
	    
	    service.shutDown();
	}
	
	private void testSingleClient() throws IOException, DFSException, InterruptedException
	{
		DFSService service = clients.get( 0 );
		
		DFSUtils.existFile( "./Clients/ResourcesClient1/Images/photoshop.pdf", true );
		
		String file = "not_present.txt";
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
		
		assertEquals( service.put( "not_present.txt" ), false );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( "not_present.txt" ), false );
		System.out.println( "\n\n" );
		
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( file ), true );
		assertEquals( service.getFile( file ), null );
	}
	
	private void testDeleteFolder() throws IOException, DFSException
	{
	    DFSService service = clients.get( 0 );
        
        DFSUtils.existFile( "./Clients/ResourcesClient1/ToDelete/sub_1/file1.txt", true );
        DFSUtils.existFile( "./Clients/ResourcesClient1/ToDelete/sub_1/file2.txt", true );
        service.reloadDB();
        
        assertTrue( service.delete( "./Clients/ResourcesClient1/ToDelete" ) );
    }
	
	private void stressTest() throws InterruptedException
    {
    	List<Thread> clientThreads = new ArrayList<>( clients.size() );
    	
    	for(int i = 0; i < clients.size(); i++) {
    		final int index = i;
    		Thread t = new Thread() {
    			@Override
    			public void run() { try { testMultipleClients( index ); } catch (Exception e) {e.printStackTrace();} }
    		};
    		t.start();
    		clientThreads.add( t );
    	}
    	
    	// Wait the termination of all the clients.
    	for(Thread t : clientThreads)
    		t.join();
    }
	
	private void testMultipleClients( final int index ) throws IOException, DFSException, InterruptedException
	{
		DFSService service = clients.get( index );
		
		DFSUtils.existFile( "./Clients/ResourcesClient" + (index + 1) + "/Images/photoshop.pdf", true );
		
		service.get( "not_present.txt" );
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
	
    public void testAntiEntropy() throws IOException, DFSException, InterruptedException
	{
		System.out.println( "\n\nTEST ANTI ENTROPY" );
		
		String file = "test3.txt";
		DFSUtils.existFile( "./Clients/ResourcesClient1/" + file, true );
		
		modifyTextFile( "./Clients/ResourcesClient1/" + file );
		assertTrue( clients.get( 0 ).put( file ) );
		//assertTrue( clients.get( 0 ).put( "test4.txt" ) );
		
		System.out.println( "Waiting..." );
		// Wait the necessary time before to check if the last node have received the file.
		Thread.sleep( AntiEntropyThread.EXCH_TIMER * 2 + 1000 );
		System.out.println( "NODE: " + members.get( 0 + NUMBER_OF_BALANCERS ) +
							", FILE: " + servers.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
		
		assertNotNull( servers.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ) );
		
		/*assertTrue( clients.get( 0 ).delete( file ) );
		//assertTrue( clients.get( 0 ).delete( "test4.txt" ) );
		System.out.println( "Waiting..." );
        // Wait the necessary time before to check if the last node have received the updated file.
        Thread.sleep( AntiEntropySenderThread.EXCH_TIMER * 2 + 1000 );
        System.out.println( "NODE: " + members.get( 0 + NUMBER_OF_BALANCERS ) +
                            ", FILE: " + servers.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );
        assertNull( servers.get( 0 + NUMBER_OF_BALANCERS ).getFile( file ) );*/
	}
	
	private void modifyTextFile( final String file ) throws IOException
	{
		PrintWriter writer = new PrintWriter( file, StandardCharsets.UTF_8.name() );
		writer.println( new Date().toString() );
		writer.flush();
		writer.close();
	}
	
    private void testHintedHandoff() throws IOException, DFSException, InterruptedException
    {
		System.out.println( "Starting Hinted Handoff test..." );
    	String file = "chord_sigcomm.pdf";
    	DFSUtils.existFile( "./Clients/ResourcesClient1/" + file, true );
    	
    	int index = 4 + NUMBER_OF_BALANCERS;
    	String hh = servers.get( index ).getAddress() + ":" + servers.get( index ).getPort();
    	servers.get( index ).close();
    	System.out.println( "Node: " + members.get( index ) + " closed." );
    	
    	Thread.sleep( 2000 );
    	
    	assertTrue( clients.get( 0 ).put( file ) );
    	System.out.println( "\n\n" );
    	Thread.sleep( 2000 );
    	
    	//assertEquals( service.get( file ), service.getFile( file ) );
    	System.out.println( "HH: " + servers.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ) );
    	assertEquals( servers.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff(), hh );
    }
    
    @After
    public void close() throws InterruptedException
    {
        if(clients != null) {
            for(DFSService service : clients)
                service.shutDown();
        }
        
        if(servers != null) {
            for(DFSNode node : servers)
                node.close();
            for(DFSNode node : servers)
                node.join();
        }
        
        System.exit( 0 );
    }
}
