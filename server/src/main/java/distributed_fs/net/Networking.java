/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
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
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketOutputStream;

public class Networking
{
    private ThroughputThread throughput_t;
    
    public static final byte[] FALSE = new byte[]{ (byte) 0x0 };
    public static final byte[] TRUE = new byte[]{ (byte) 0x1 };
    
    
    
    /**
     * Creates a new message.
     * 
     * @param currentData        current data
     * @param toAdd                next bytes to add
     * @param addLength            {@code true} if the length of the toAdd message have to be added,
     *                             {@code false} otherwise
    */
    public byte[] createMessage( byte[] currentData, byte[] toAdd, boolean addLength )
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
    
    public class Session implements Closeable
    {
        private final BufferedInputStream in;
        private final BufferedOutputStream out;
        private final Socket socket;
        private final String endPointAddress;
        
        private boolean close = false;
        
        private final byte[] INT = new byte[Integer.BYTES];
        
        
        
        
        public Session( BufferedInputStream in,
                        BufferedOutputStream out,
                        Socket socket ) {
            this( in, out, socket, "" );
        }
        
        public Session( BufferedInputStream in,
                        BufferedOutputStream out,
                        Socket socket,
                        String srcAddress ) {
            this.in = in;
            this.out = out;
            this.socket = socket;
            this.endPointAddress = srcAddress;
        }
        
        public BufferedInputStream getInputStream(){ return in; }
        public BufferedOutputStream getOutputStream(){ return out; }
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
        public void setSoTimeout( int timeout ) throws IOException {
            socket.setSoTimeout( timeout );
        }
        
        /** 
         * Sends a new message.
         * 
         * @param message        message to transmit
         * @param tryCompress    {@code true} if the data could be sent compressed,
         *                         {@code false} otherwise
        */
        public void sendMessage( Message message, boolean tryCompress ) throws IOException
        {
            sendMessage( DFSUtils.serializeObject( message ), tryCompress );
        }
        
        /** 
         * Sends a new message.
         * 
         * @param data            data to transmit
         * @param tryCompress    {@code true} if the data could be sent compressed,
         *                         {@code false} otherwise
        */
        public void sendMessage( byte[] data, boolean tryCompress ) throws IOException
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
        
        private void doWrite( byte[] data ) throws IOException
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
           return receiveMessage( null );
        }
        
        /** 
         * Receives a new message.
        */
        public <T extends Message> T receiveMessage( TransferSpeed callback ) throws IOException
        {
            T message = DFSUtils.deserializeObject( receive( callback ) );
            return message;
        }
        
        /** 
         * Receives a new message as a list of bytes.    
        */
        public byte[] receive() throws IOException
        {
            return receive( null );
        }
        
        /** 
         * Receives a new message as a list of bytes.
         * 
         * @param callback    object used to be notified about the download time of the file
        */
        public byte[] receive( TransferSpeed callback ) throws IOException
        {
            try {
                final int size = readInt();
                boolean decompress = (in.read() == (byte) 0x1);
                byte[] data = new byte[size];
                
                FileSpeed file = new FileSpeed( size, callback );
                if(callback != null) {
                    if(throughput_t == null) {
                        throughput_t = new ThroughputThread();
                        throughput_t.start();
                    }
                    
                    throughput_t.addFile( file );
                }
                
                while(file.bytesReceived < size) {
                    int bytesRead = in.read( data, file.bytesReceived, (size - file.bytesReceived) );
                    if(bytesRead >= 0)
                        file.bytesReceived += bytesRead;
                    //System.out.println( "Read " + bytesRead + " bytes." );
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
        
        /**
         * Reads the next {@link Integer#BYTES} bytes from the
         * socket.
        */
        private int readInt() throws IOException
        {
            in.read( INT );
            return DFSUtils.byteArrayToInt( INT );
        }
        
        @Override
        public void close()
        {
            if(!close) {
                try { in.close(); }
                catch( IOException e ){}
                try { out.close();}
                catch( IOException e ){}
                try { socket.close();}
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
        private List<Session> sessions = new ArrayList<>( 32 );
        private boolean closed = false;
        
        
        
        public TCPnet() {}
        
        /**
         * Constructor used to instantiate the {@code ServerSocket} object.
         * 
         * @param localAddress    
         * @param localPort        
        */
        public TCPnet( String localAddress, int localPort ) throws IOException
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
        public void setSoTimeout( int timeout )
        {
            soTimeout = timeout;
            if(servSocket != null) {
                try { servSocket.setSoTimeout( timeout ); }
                catch( SocketException e ) { e.printStackTrace(); }
            }
        }
        
        /**
         * Waits for a new connection.
        */
        public Session waitForConnection() throws IOException
        {
            if(servSocket == null)
                throw new IOException( "Local address and port cannot be empty." );
            
            // The address and port values have been already
            // defined in the constructor.
            return waitForConnection( null, 0 );
        }
        
        /**
         * Waits for a new connection.
         * 
         * @param localAddress
         * @param localPort
        */
        public Session waitForConnection( String localAddress, int localPort ) throws IOException
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
            BufferedInputStream in = new BufferedInputStream( socket.getInputStream() );
            BufferedOutputStream out = new BufferedOutputStream( socket.getOutputStream() );
            
            Session session = new Session( in, out, socket, srcAddress );
            sessions.add( session );
            
            return session;
        }
        
        /**
         * Tries a connection with the remote host address.
         * This has the same effect as invoking: {@code tryConnect( address, port, 0 )}.
        */
        public Session tryConnect( String address, int port ) throws IOException {
            return tryConnect( address, port, 0 );
        }
        
        /** 
         * Tries a connection with the remote host address.
         * 
         * @param address    the remote address
         * @param port        the remote port
         * @param timeOut    the connection will remain blocked
         *                     for a maximum of {@code timeOut} milliseconds.
         *                     0 means infinite time.
        */
        public Session tryConnect( String address, int port, int timeOut ) throws IOException
        {
            Socket socket = new Socket();
            
            try {
                socket.connect( new InetSocketAddress( address, port ), timeOut );
            } catch( SocketTimeoutException e ) {
                socket.close();
                return null;
            }
            
            socket.setTcpNoDelay( true );
            
            BufferedInputStream in = new BufferedInputStream( socket.getInputStream() );
            BufferedOutputStream out = new BufferedOutputStream( socket.getOutputStream() );
            Session session = new Session( in, out, socket, address );
            
            return session;
        }

        @Override
        public void close()
        {
            if(!closed) {
                closed = true;
                try {
                    for(Session session : sessions)
                        session.close();
                    sessions.clear();
                    
                    if(servSocket != null)
                        servSocket.close();
                }
                catch( IOException e ){}
            }
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
        
        private boolean closed = false;
        
        private static final int PORT_GROUP = 4680;
        private static final String multicastIP = "230.0.1.0";
        
        public UDPnet() throws IOException
        {
            udpSocket = new MulticastSocket();
            udpSocket.setLoopbackMode( true );
            mAddress = InetAddress.getByName( multicastIP );
        }
        
        public UDPnet( String localAddress, int localPort ) throws IOException
        {
            udpSocket = new MulticastSocket( new InetSocketAddress( localAddress, localPort ) );
            udpSocket.setLoopbackMode( true );
            mAddress = InetAddress.getByName( multicastIP );
        }
        
        public void setSoTimeout( int timeout ) throws IOException
        {
            //soTimeout = timeout;
            udpSocket.setSoTimeout( timeout );
        }
        
        /** 
         * Tries a connection with the remote host address.
         * 
         * @param address    the remote address
         * @param port        the remote port
        */
        public void tryConnect( String address, int port ) throws IOException
        {
            udpSocket.connect( new InetSocketAddress( address, port ) );
            //udpSocket.setSoTimeout( soTimeout );
        }
        
        /** 
         * Joins a multicast group.
         * 
         * @param _address        interface used for the multicast send/receive
        */
        public void joinMulticastGroup( InetAddress _address ) throws IOException
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
        public void sendMessage( byte[] message, InetAddress address, int port ) throws IOException
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
            if(!closed) {
                closed = true;
                
                if(!udpSocket.isClosed() && joinMulticastGroup) {
                    try { udpSocket.leaveGroup( mAddress ); }
                    catch( IOException e ) { e.printStackTrace(); }
                }
                
                udpSocket.close();
            }
        }
    }
    
    public static class RUDPnet extends Networking implements Closeable
    {
        private ReliableServerSocket servSocket;
        private int soTimeout = 0;
        /** Keep track of all the opened RUDP sessions. */ 
        private List<Session> sessions = new ArrayList<>( 32 );
        private boolean closed = false;
        
        
        
        public RUDPnet() {}
        
        /**
         * Constructor used to instantiate the {@code ServerSocket} object.
         * 
         * @param localAddress  
         * @param localPort     
        */
        public RUDPnet( String localAddress, int localPort ) throws IOException
        {
            InetAddress iAddress = InetAddress.getByName( localAddress );
            servSocket = new ReliableServerSocket( localPort, 0, iAddress );
        }
        
        /**
         * Enable/disable {@code SO_TIMEOUT} with the specified timeout, in milliseconds.
         * With this option set to a non-zero timeout, a call to accept() for this ServerSocket will block for only this amount of time. 
         * If the timeout expires, a java.net.SocketTimeoutException is raised, though the ServerSocket is still valid.
         * The option must be enabled prior to entering the blocking operation to have effect.
         * The timeout must be > 0. A timeout of zero is interpreted as an infinite timeout.
        */
        public void setSoTimeout( int timeout )
        {
            soTimeout = timeout;
            if(servSocket != null)
                servSocket.setSoTimeout( timeout );
        }
        
        /**
         * Waits for a new connection.
        */
        public Session waitForConnection() throws IOException
        {
            if(servSocket == null)
                throw new IOException( "Local address and port must be defined." );
            
            // The address and port values have been already
            // defined in the constructor.
            return waitForConnection( null, 0 );
        }
        
        /**
         * Waits for a new connection.
         * 
         * @param localAddress
         * @param localPort
        */
        public Session waitForConnection( String localAddress, int localPort ) throws IOException
        {
            if(servSocket == null) {
                InetAddress iAddress = InetAddress.getByName( localAddress );
                servSocket = new ReliableServerSocket( localPort, 0, iAddress );
                servSocket.setSoTimeout( soTimeout );
            }
            
            Socket socket;
            try{ socket = servSocket.accept(); }
            catch( SocketTimeoutException e ) { return null; }
            
            String srcAddress = ((InetSocketAddress) socket.getRemoteSocketAddress()).getHostString();
            BufferedInputStream in = new BufferedInputStream( socket.getInputStream() );
            BufferedOutputStream out = new BufferedOutputStream( (ReliableSocketOutputStream) socket.getOutputStream() );
            
            Session session = new Session( in, out, socket, srcAddress );
            sessions.add( session );
            
            return session;
        }
        
        /**
         * Tries a connection with the remote host address.
         * This has the same effect as invoking: {@code tryConnect( address, port, 0 )}.
        */
        public Session tryConnect( String address, int port ) throws IOException {
            return tryConnect( address, port, 0 );
        }
        
        /** 
         * Tries a connection with the remote host address.
         * 
         * @param address   the remote address
         * @param port      the remote port
         * @param timeOut   the connection will remain blocked
         *                  for a maximum of {@code timeOut} milliseconds.
         *                  0 means infinite time.
        */
        public Session tryConnect( String address, int port, int timeOut ) throws IOException
        {
            ReliableSocket socket = new ReliableSocket();
            
            try {
                socket.connect( new InetSocketAddress( address, port ), timeOut );
            } catch( SocketTimeoutException e ) {
                socket.close();
                return null;
            }
            
            BufferedInputStream in = new BufferedInputStream( socket.getInputStream() );
            BufferedOutputStream out = new BufferedOutputStream( (ReliableSocketOutputStream) socket.getOutputStream() );
            Session session = new Session( in, out, socket, address );
            
            return session;
        }

        @Override
        public void close()
        {
            if(!closed) {
                closed = true;
                for(Session session : sessions)
                    session.close();
                sessions.clear();
                
                // FIXME Bug on the MR-UDP library.
                // FIXME Loop of exceptions start internally (in some Thread).
                // FIXME Add this lines when the bug will be fixed.
                //if(servSocket != null)
                    //servSocket.close();
            }
        }
    }
    
    private static class FileSpeed
    {
        public final int fileSize;
        public final TransferSpeed callback;
        public long startTime;
        public int bytesReceived;
        
        public FileSpeed( int fileSize, TransferSpeed callback )
        {
            this.fileSize = fileSize;
            this.callback = callback;
            
            startTime = System.currentTimeMillis();
        }
    }
    
    /**
     * Thread used to compute the throughput of the communication,
     * in real time.
    */
    private static class ThroughputThread extends Thread
    {
        private final List<FileSpeed> files = new ArrayList<>( 32 );
        
        
        public ThroughputThread()
        {
            setName( "ThroughputThread" );
            setDaemon( true );
        }
        
        public void addFile( FileSpeed file )
        {
            synchronized( files) {
                files.add( file );
            }
        }
        
        private void removeFile( int index )
        {
            synchronized( files) {
                files.remove( index );
            }
        }
        
        @Override
        public void run()
        {
            while(true) {
                try { Thread.sleep( 1000 ); }
                catch( InterruptedException e ) {}
                
                for(int i = files.size() - 1; i >= 0; i--) {
                    if(computeThroughput( files.get( i ) ))
                        removeFile( i );
                }
            }
        }
        
        /**
         * Computes the actual throughput of the file.
         * 
         * @param file     the current file
         * 
         * @return {@code true} if the file has been completely download,
         *         {@code false} otherwise
        */
        private boolean computeThroughput( FileSpeed file )
        {
            if(file.bytesReceived >= file.fileSize) // File completely downloaded.
                return true;
            else {
                long end = System.currentTimeMillis();
                double throughput = file.bytesReceived / (end - file.startTime);
                //System.out.println( "Throughput: " + (throughput / 1024f / 1024f) + " MB/s" );
                
                file.callback.update( file.fileSize - file.bytesReceived, throughput );
                return false;
            }
        }
    }
}
