/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import distributed_fs.net.messages.Message;
import distributed_fs.utils.DFSUtils;

public class Networking
{
	public static final byte[] FALSE = new byte[]{ (byte) 0x0 };
	public static final byte[] TRUE = new byte[]{ (byte) 0x1 };
	
	/**
	 * Creates a new message.
	 * 
	 * @param currentData		current data
	 * @param toAdd				next bytes to add
	 * @param addLength			{@code true} if the length of the toAdd message have to be added,
	 * 							{@code false} otherwise
	*/
	public byte[] createMessage( final byte[] currentData, final byte[] toAdd, final boolean addLength )
	{
		ByteBuffer buffer;
		
		if(currentData == null) {
			if(addLength) {
				buffer = ByteBuffer.allocate( Integer.BYTES + toAdd.length );
				buffer.putInt( toAdd.length ).put( toAdd );
			}
			else {
				buffer = ByteBuffer.allocate( toAdd.length );
				buffer.put( toAdd );
			}
		}
		else {
			if(addLength) {
				buffer = ByteBuffer.allocate( currentData.length + Integer.BYTES + toAdd.length );
				buffer.put( currentData ).putInt( toAdd.length ).put( toAdd );
			}
			else {
				buffer = ByteBuffer.allocate( currentData.length + toAdd.length );
				buffer.put( currentData ).put( toAdd );
			}
		}
		
		return buffer.array();
	}
	
	public class TCPSession implements Closeable
	{
		private final DataInputStream in;
		private final DataOutputStream out;
		private final Socket socket;
		private final String endPointAddress;
		private boolean close = false;
		
		public TCPSession( final DataInputStream in,
						   final DataOutputStream out,
						   final Socket socket ) {
			this( in, out, socket, "" );
		}
		
		public TCPSession( final DataInputStream in,
						   final DataOutputStream out,
						   final Socket socket,
						   final String srcAddress ) {
			this.in = in;
			this.out = out;
			this.socket = socket;
			this.endPointAddress = srcAddress;
		}
		
		public DataInputStream getInputStream(){ return in; }
		public DataOutputStream getOutputStream(){ return out; }
		public Socket getSocket(){ return socket; }
		public String getEndPointAddress(){ return endPointAddress; }

        public boolean isClosed() { return close; }
		
		/**
		 * Enable/disable {@code SO_TIMEOUT} with the specified timeout, in milliseconds.
		 * With this option set to a non-zero timeout, a call to accept() for this ServerSocket will block for only this amount of time. 
		 * If the timeout expires, a java.net.SocketTimeoutException is raised, though the ServerSocket is still valid.
		 * The option must be enabled prior to entering the blocking operation to have effect.
		 * The timeout must be > 0. A timeout of zero is interpreted as an infinite timeout.
		*/
		public void setSoTimeout( final int timeout ) throws IOException {
			socket.setSoTimeout( timeout );
		}
		
		/** 
		 * Sends a new message.
		 * 
		 * @param message		message to transmit
		 * @param tryCompress	{@code true} if the data could be sent compressed,
		 * 						{@code false} otherwise
		*/
		public void sendMessage( final Message message, final boolean tryCompress ) throws IOException
		{
			sendMessage( DFSUtils.serializeObject( message ), tryCompress );
		}
		
		/** 
		 * Sends a new message.
		 * 
		 * @param data			data to transmit
		 * @param tryCompress	{@code true} if the data could be sent compressed,
		 * 						{@code false} otherwise
		*/
		public void sendMessage( final byte[] data, final boolean tryCompress ) throws IOException
		{
			byte[] message;
			
			if(tryCompress)
				message = DFSUtils.compressData( data );
			else
				message = data;
			
			ByteBuffer buffer;
			if(data.length <= message.length) {
				buffer = ByteBuffer.allocate( Integer.BYTES + Byte.BYTES + data.length );
				buffer.putInt( data.length ).put( (byte) 0x0 ).put( data );
			}
			else {
				buffer = ByteBuffer.allocate( Integer.BYTES + Byte.BYTES + message.length );
				buffer.putInt( message.length ).put( (byte) 0x1 ).put( message );
			}
			
			doWrite( buffer.array() );
		}
		
		private void doWrite( final byte[] data ) throws IOException
		{
			try{ 
				out.write( data );
				out.flush();
			}
			catch( IOException e ) {
				close();
				throw e;
			}
		}
		
		/** 
         * Receives a new message.
        */
        public <T extends Message> T receiveMessage() throws IOException
		{
		    T message = DFSUtils.deserializeObject( receive() );
		    return message;
		}
		
		/** 
		 * Receives a new message as a list of bytes.
		*/
		public byte[] receive() throws IOException
		{
			try {
				int size = in.readInt();
				//System.out.println( "Read: " + data.length + " bytes" );
				boolean decompress = (in.read() == (byte) 0x1);
			
				byte[] data = new byte[size];
				
				int current = 0;
				while(current < size) {
					int bytesRead = in.read( data, current, (size - current) );
					if(bytesRead >= 0)
						current += bytesRead;
				}
				
				if(decompress)
					return DFSUtils.decompressData( data );
				else
					return data;
			}
			catch( IOException e ) {
				close();
				throw e;
			}
		}
		
		@Override
		public void close()
		{
			if(!close) {
				try {
					in.close();
					out.close();
					socket.close();
				}
				catch( IOException e ){}
				
				close = true;
			}
		}
	}
	
	public static class TCPnet extends Networking implements Closeable
	{
		private ServerSocket servSocket;
		private int soTimeout = 0;
		/** Keep track of all the opened TCP sessions. */ 
		private List<TCPSession> sessions = new ArrayList<>( 32 );
		
		public TCPnet() {}
		
		/**
		 * Constructor used to instantiate the {@code ServerSocket} object.
		 * 
		 * @param localAddress	
		 * @param localPort		
		*/
		public TCPnet( final String localAddress, final int localPort ) throws IOException
		{
			InetAddress iAddress = InetAddress.getByName( localAddress );
			servSocket = new ServerSocket( localPort, 0, iAddress );
		}
		
		/**
		 * Enable/disable {@code SO_TIMEOUT} with the specified timeout, in milliseconds.
		 * With this option set to a non-zero timeout, a call to accept() for this ServerSocket will block for only this amount of time. 
		 * If the timeout expires, a java.net.SocketTimeoutException is raised, though the ServerSocket is still valid.
		 * The option must be enabled prior to entering the blocking operation to have effect.
		 * The timeout must be > 0. A timeout of zero is interpreted as an infinite timeout.
		*/
		public void setSoTimeout( final int timeout ) {
			soTimeout = timeout;
			if(servSocket != null) {
				try { servSocket.setSoTimeout( timeout ); }
				catch( SocketException e ) { e.printStackTrace(); }
			}
		}
		
		/**
		 * Waits for a new connection.
		*/
		public TCPSession waitForConnection() throws IOException
		{
			// The address and port values have been already
			// setted in the constructor.
			return waitForConnection( null, 0 );
		}
		
		/**
		 * Waits for a new connection.
		 * 
		 * @param localAddress
		 * @param localPort
		*/
		public TCPSession waitForConnection( final String localAddress, final int localPort ) throws IOException
		{
			if(servSocket == null) {
				InetAddress iAddress = InetAddress.getByName( localAddress );
				servSocket = new ServerSocket( localPort, 0, iAddress );
				servSocket.setSoTimeout( soTimeout );
			}
			
			Socket socket;
			try{ socket = servSocket.accept(); }
			catch( SocketTimeoutException e ) { return null; }
			socket.setTcpNoDelay( true );
			
			String srcAddress = ((InetSocketAddress) socket.getRemoteSocketAddress()).getHostString();
			DataInputStream in = new DataInputStream( socket.getInputStream() );
			DataOutputStream out = new DataOutputStream( socket.getOutputStream() );
			
			TCPSession session = new TCPSession( in, out, socket, srcAddress );
			sessions.add( session );
			
			return session;
		}
		
		/**
		 * Tries a connection with the remote host address.
		 * This has the same effect as invoking: {@code tryConnect( address, port, 0 )}.
		*/
		public TCPSession tryConnect( final String address, final int port ) throws IOException
		{
			return tryConnect( address, port, 0 );
		}
		
		/** 
		 * Tries a connection with the remote host address.
		 * 
		 * @param address	the remote address
		 * @param port		the remote port
		 * @param timeOut	the connection will remain blocked
		 * 					for a maximum of {@code timeOut} milliseconds.
		 * 					0 means infinite time.
		*/
		public TCPSession tryConnect( final String address, final int port, final int timeOut ) throws IOException
		{
			Socket socket = new Socket();
			
			try {
				socket.connect( new InetSocketAddress( address, port ), timeOut );
			} catch( SocketTimeoutException e ) {
				socket.close();
				return null;
			}
			
			socket.setTcpNoDelay( true );
			
			DataInputStream in = new DataInputStream( socket.getInputStream() );
			DataOutputStream out = new DataOutputStream( socket.getOutputStream() );
			TCPSession session = new TCPSession( in, out, socket, address );
			
			return session;
		}

		@Override
		public void close()
		{
			try {
				for(TCPSession session : sessions)
					session.close();
				sessions.clear();
				
				if(servSocket != null)
					servSocket.close();
			}
			catch( IOException e ){}
		}
	}
	
	public static class UDPnet extends Networking implements Closeable
	{
		//private int soTimeout = 0;
		private MulticastSocket udpSocket;
		private InetAddress mAddress;
		private boolean joinMulticastGroup = false;
		
		private String srcAddress;
		private int srcPort;
		
		private static final int PORT_GROUP = 4680;
		private static final String multicastIP = "230.0.1.0";
		
		public UDPnet() throws IOException
		{
			udpSocket = new MulticastSocket();
			udpSocket.setLoopbackMode( true );
			mAddress = InetAddress.getByName( multicastIP );
		}
		
		public UDPnet( final String localAddress, final int localPort ) throws IOException
		{
			udpSocket = new MulticastSocket( new InetSocketAddress( localAddress, localPort ) );
			udpSocket.setLoopbackMode( true );
			mAddress = InetAddress.getByName( multicastIP );
		}
		
		public void setSoTimeout( final int timeout ) throws IOException
		{
			//soTimeout = timeout;
			udpSocket.setSoTimeout( timeout );
		}
		
		/** 
		 * Tries a connection with the remote host address.
		 * 
		 * @param address	the remote address
		 * @param port		the remote port
		*/
		public void tryConnect( final String address, final int port ) throws IOException
		{
			udpSocket.connect( new InetSocketAddress( address, port ) );
			//udpSocket.setSoTimeout( soTimeout );
		}
		
		/** 
		 * Joins a multicast group.
		 * 
		 * @param _address		interface used for the multicast send/receive
		*/
		public void joinMulticastGroup( final InetAddress _address ) throws IOException
		{
			close();
			
			udpSocket = new MulticastSocket( PORT_GROUP );
			udpSocket.setLoopbackMode( true );

			// interface used for the multicast send/receive
			udpSocket.setInterface( _address );
			udpSocket.joinGroup( mAddress );
			joinMulticastGroup = true;
			
			//udpSocket.setSoTimeout( soTimeout );
		}
		
		public MulticastSocket getSocket(){ return udpSocket; }
		public String getSrcAddress(){ return srcAddress; }
		public int getSrcPort(){ return srcPort; }
		
		/**
		 * Sends a new message to the specified address.
		 * 
		 * @param message	
		 * @param address	
		 * @param port		
		*/
		public void sendMessage( final byte[] message, final InetAddress address, final int port ) throws IOException
		{
			ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES + message.length );
			buffer.putInt( message.length );
			buffer.put( message );
			byte[] data = buffer.array();
			
			DatagramPacket dPacket = new DatagramPacket( data, data.length, address, port );
			udpSocket.send( dPacket );
		}
		
		/**
		 * Sends a new message over the multicast channel.
		 * 
		 * @param message	
		*/
		public void sendMulticastMessage( byte[] message ) throws IOException
		{
			ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES + message.length );
			buffer.putInt( message.length );
			buffer.put( message );
			byte[] data = buffer.array();
			
			DatagramPacket dPacket = new DatagramPacket( data, data.length, mAddress, PORT_GROUP );
			udpSocket.send( dPacket );
		}
		
		/** 
		 * Waits for a new incoming message.
		*/
		public byte[] receiveMessage() throws IOException
		{
			byte packet[] = new byte[udpSocket.getReceiveBufferSize()];
			DatagramPacket dPacket = new DatagramPacket( packet, packet.length );
			//udpSocket.setSoTimeout( soTimeout );
			
			try { udpSocket.receive( dPacket ); }
			catch( SocketTimeoutException e ) { return null; }
			
			srcAddress = dPacket.getAddress().getHostAddress();
			srcPort = dPacket.getPort();
			
			ByteBuffer buffer = ByteBuffer.wrap( dPacket.getData() );
			return DFSUtils.getNextBytes( buffer );
		}
		
		@Override
		public void close()
		{
		    if(!udpSocket.isClosed() && joinMulticastGroup) {
		        try { udpSocket.leaveGroup( mAddress ); }
		        catch( IOException e ) { e.printStackTrace(); }
		    }
		    
			udpSocket.close();
		}
	}
}