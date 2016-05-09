/**
 * @author Stefano Ceccotti
*/

package distributed_fs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import distributed_fs.anti_entropy.AntiEntropySenderThread;
import distributed_fs.client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.overlay.DFSnode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class Tests
{
	private static String myIpAddress;
	
	private DFSService service;
	private List<GossipMember> members;
	private List<DFSnode> nodes;
	
	private static final int NUMBER_OF_BALANCERS = 2;
	private static final int NUMBER_OF_NODES = 5;
	
	public static void main( final String[] args ) throws Exception
	{
		/*VectorClock clock = new VectorClock();
		for(int i = 0; i < 4000000; i++)
			clock.incrementVersion( "" + i );
		
		/*RemoteFile file = new RemoteFile( "test2.txt", clock, false, false, "./Resources/" );
		System.out.println( "FILE: " + file + "\nTHEN: " + new RemoteFile( file.read() ) );
		System.out.println( "SIZE_READ: " + file.read().length );
		System.out.println( "SIZE_SRLZ: " + Utils.serializeObject( file ).length );*/
		/*DistributedFile file = new DistributedFile( "test2.txt", "./Resources/", clock );
		file.setHintedHandoff( "pippo" );
		file.setDeleted( true );
		System.out.println( "FILE: " + file + "\nTHEN: " + new DistributedFile( file.read() ) );
		System.out.println( "SIZE_READ: " + file.read().length );
		System.out.println( "SIZE_SRLZ: " + Utils.serializeObject( file ).length );
		
		System.out.println( "CLOCK: " + clock );
		//clock = new VectorClock( clock.read() );
		System.out.println( "CLOCK: " + Utils.serializeObject( clock ).length );
		//System.out.println( "SIZE: " + Utils.serializeObject( clock ).length );*/
		
		new Tests();
	}
	
	public Tests() throws Exception
	{
		// First test when the service is down.
		service = new DFSService( null, null, null );
		assertFalse( service.start() );
		service.shutDown();
		
		startSystem();
		
		Thread.sleep( 2000 );
		
		//testClientOperations();
		//testHintedHandoff();
		testAntiEntropy();
		
		close();
	}
	
	@Before
	public void startSystem() throws IOException, JSONException, SQLException, InterruptedException, DFSException
	{
		// enumerate all the network intefaces
		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
		for(NetworkInterface netInt : Collections.list( nets )) {
			if(netInt.getName().equals( "eth0" )) {
				// enumerate all the IP address associated with it
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
		service = new DFSService( myIpAddress, members, null, null );
		assertTrue( service.start() );
	}
	
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
	
	@Test
	public void testClientOperations() throws IOException, JSONException, DFSException, InterruptedException
	{
		String file = "";
		assertEquals( service.get( file ), service.getFile( file ) );
		file = "./Resources/Images/photoshop.pdf";
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.put( file ), true );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.get( file ), service.getFile( file ) );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
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
		int index = 2 + NUMBER_OF_BALANCERS;
		String hh = nodes.get( index ).getAddress() + ":" + nodes.get( index ).getPort();
		nodes.get( index ).closeResources();
		System.out.println( "Node: " + members.get( index ) + " closed." );
		
		Thread.sleep( 2000 );
		
		assertTrue( service.put( file ) );
		System.out.println( "\n\n" );
		Thread.sleep( 2000 );
		
		//assertEquals( service.get( file ), service.getFile( file ) );
		assertEquals( nodes.get( 4 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff(), hh );
		//System.out.println( "HH: " + nodes.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff() );
	}
	
	@Test
	public void testAntiEntropy() throws IOException, JSONException, DFSException, InterruptedException, SQLException
	{
		//System.out.println( "SUCCESSOR: " + nodes.get( 0 + NUMBER_OF_BALANCERS ).getSuccessor() );
		System.out.println( "\n\nTEST ANTI ENTROPY" );
		
		String file = "test3.txt";
		
		modifyTextFile( "./Resources/" + file );
		assertTrue( service.put( file ) );
		
		System.out.println( "Waiting..." );
		// Wait the necessary time before to check if the last node have received the file.
		Thread.sleep( AntiEntropySenderThread.EXCH_TIMER * 2 );
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
	
	// TODO aggiungere test in cui si modifica il database e si fanno delle verifiche
	
	@Test
	public void stressTest()
	{
		// TODO lanciare diversi client e testare diverse operazioni
		
	}

	@After
	public void close()
	{
		service.shutDown();
		
		for(int i = 0; i < nodes.size(); i++)
			nodes.get( i ).closeResources();
	}
}