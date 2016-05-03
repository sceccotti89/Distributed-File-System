/**
 * @author Stefano Ceccotti
*/

package distributed_fs.test;

import static org.junit.Assert.*;

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

import distributed_fs.client.DFSService;
import distributed_fs.exception.DFSException;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;
import gossiping.RemoteGossipMember;

public class TestClient
{
	private static String myIpAddress;
	private List<GossipMember> startupMembers;
	
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
		
		new TestClient();
	}
	
	public TestClient() throws Exception
	{
		// First test when the service is down
		DFSService service = new DFSService();
		assertFalse( service.start() );
		service.shutDown();
		
		runServers( myIpAddress );
		
		Thread.sleep( 2000 );
		
		testClientOperations( myIpAddress );
	}
	
	@Before
	private void runServers( final String myIpAddress ) throws IOException, JSONException, SQLException
	{
		// Create the gossip members and put them in a list and give them a port number starting with 2000.
		startupMembers = new ArrayList<>();
		int k = 100;
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++, k++) {
			int port = 8000 + (i * k);
			String id = Utils.bytesToHex( Utils.getNodeId( 0, myIpAddress + ":" + port ).array() );
			startupMembers.add( new RemoteGossipMember( myIpAddress, port, id, 3, GossipMember.LOAD_BALANCER ) );
		}
		for(int i = 0 ; i < NUMBER_OF_NODES; i++, k++) {
			int port = 8000 + (i * k) + NUMBER_OF_BALANCERS;
			String id = Utils.bytesToHex( Utils.getNodeId( 0, myIpAddress + ":" + port ).array() );
			startupMembers.add( new RemoteGossipMember( myIpAddress, port, id, 3, GossipMember.STORAGE ) );
		}
		
		// start the load balancer nodes
		for(int i = 0; i < NUMBER_OF_BALANCERS; i++) {
			LoadBalancer node = new LoadBalancer( startupMembers, startupMembers.get( i ).getPort(), myIpAddress );
			new Thread() {
				public void run() { try { node.launch(); } catch (IOException | JSONException | SQLException e) {e.printStackTrace();} }
			}.start();
		}
		
		// start the storage nodes
		for(int i = 0; i < NUMBER_OF_NODES; i++) {
			GossipMember member = startupMembers.get( i + NUMBER_OF_BALANCERS );
			StorageNode node = new StorageNode( startupMembers, member.getId(), myIpAddress, member.getPort() );
			new Thread() {
				public void run() { try { node.launch(); } catch (IOException | JSONException | SQLException e) {e.printStackTrace();} }
			}.start();
		}
	}
	
	@After
	private void testClientOperations( final String myIpAddress ) throws IOException, JSONException, DFSException, InterruptedException
	{
		// TODO fare piu' test possibili
		DFSService service = new DFSService( myIpAddress, startupMembers );
		assertTrue( service.start() );
		
		String name = "";
		assertEquals( service.get( name ), service.getFile( name ) );
		name = "./Resources/Images/photoshop.pdf";
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.put( name ), true );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.get( name ), service.getFile( name ) );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.put( name ), true );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.put( "" ), false );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( "" ), false );
		System.out.println( "\n\n" );
		Thread.sleep( 1000 );
		
		assertEquals( service.delete( "./Resources/Images/photoshop.pdf" ), true );
	}
}