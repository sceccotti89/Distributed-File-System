/**
 * @author Stefano Ceccotti
*/

package distributed_fs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
	private List<GossipMember> startupMembers;
	private List<DFSnode> nodes;
	
	private static final int NUMBER_OF_BALANCERS = 2;
	private static final int NUMBER_OF_NODES = 4;

	public static void main( final String[] args ) throws Exception
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
		
		//myIpAddress = InetAddress.getLocalHost().getHostAddress();
		
		new Tests();
	}
	
	public Tests() throws Exception
	{
		// First test when the service is down.
		service = new DFSService();
		assertFalse( service.start() );
		service.shutDown();
		
		runServers( myIpAddress );
		
		Thread.sleep( 2000 );
		
		//testClientOperations( myIpAddress );
		testHintedHandoff();
	}
	
	@Before
	public void startSystem( final String myIpAddress ) throws IOException, JSONException, SQLException
	{
		runServers( myIpAddress );
		service = new DFSService( myIpAddress, startupMembers );
	}
	
	private void runServers( final String myIpAddress ) throws IOException, JSONException, SQLException
	{
		// Create the gossip members and put them in a list.
		startupMembers = new ArrayList<>();
		
		int k = 100;
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
			int port = 8000 + (i * k);
			String id = Utils.bytesToHex( Utils.getNodeId( 0, myIpAddress + ":" + port ).array() );
			//System.out.println( "ID: " + id );
			startupMembers.add( new RemoteGossipMember( myIpAddress, port, id, 3, GossipMember.LOAD_BALANCER ) );
		}
		for(int i = 0 ; i < NUMBER_OF_NODES; i++, k++) {
			int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
			String id = Utils.bytesToHex( Utils.getNodeId( 0, myIpAddress + ":" + port ).array() );
			//System.out.println( "ID: " + id );
			startupMembers.add( new RemoteGossipMember( myIpAddress, port, id, 3, GossipMember.STORAGE ) );
		}
		
		nodes = new ArrayList<>();
		
		// Start the load balancer nodes.
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
			LoadBalancer node = new LoadBalancer( startupMembers, startupMembers.get( i ).getPort(), myIpAddress );
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (IOException | JSONException e) {e.printStackTrace();} }
			}.start();
		}
		
		// Start the storage nodes.
		for(int i = 0; i < NUMBER_OF_NODES; i++) {
			GossipMember member = startupMembers.get( i + NUMBER_OF_BALANCERS );
			StorageNode node = new StorageNode( startupMembers, member.getId(), myIpAddress, member.getPort() );
			nodes.add( node );
			new Thread() {
				@Override
				public void run() { try { node.launch(); } catch (IOException | JSONException e) {e.printStackTrace();} }
			}.start();
		}
	}
	
	@Test
	public void testClientOperations( final String myIpAddress ) throws IOException, JSONException, DFSException, InterruptedException
	{
		// TODO se il service viene avviato nell'init qui non serve
		service = new DFSService( myIpAddress, startupMembers );
		assertTrue( service.start() );
		
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
		
		assertEquals( service.delete( "./Resources/Images/photoshop.pdf" ), true );
		assertEquals( service.getFile( "./Resources/Images/photoshop.pdf" ), null );
		
		// TODO usare close
		service.shutDown();
		
		for(int i = 0; i < nodes.size(); i++)
			nodes.get( i ).closeResources();
	}
	
	@Test
	public void testHintedHandoff() throws IOException, JSONException, DFSException, InterruptedException
	{
		String file = "./Resources/chord_sigcomm.pdf";
		int index = 3 + NUMBER_OF_BALANCERS;
		String hh = nodes.get( index ).getAddress() + ":" + nodes.get( index ).getPort();
		nodes.get( index ).closeResources();
		
		Thread.sleep( 2000 );
		
		// TODO se il service viene avviato nell'init qui non serve
		service = new DFSService( myIpAddress, startupMembers );
		assertTrue( service.start() );
		
		assertEquals( service.put( file ), true );
		System.out.println( "\n\n" );
		Thread.sleep( 2000 );
		
		//assertEquals( service.get( file ), service.getFile( file ) );
		assertEquals( nodes.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff(), hh );
		//System.out.println( "HH: " + nodes.get( 1 + NUMBER_OF_BALANCERS ).getFile( file ).getHintedHandoff() );
		
		// TODO usare close
		service.shutDown();
		
		for(int i = 0; i < nodes.size(); i++)
			nodes.get( i ).closeResources();
	}
	
	@After
	public void close()
	{
		service.shutDown();
		
		for(int i = 0; i < nodes.size(); i++)
			nodes.get( i ).closeResources();
	}
}