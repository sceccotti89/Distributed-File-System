/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking;
import distributed_fs.net.Networking.Session;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.overlay.manager.QuorumThread.QuorumFile;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;
import distributed_fs.overlay.manager.anti_entropy.AntiEntropyService;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileTransfer;
import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

public class FileTransferThread extends Thread implements FileTransfer
{
    private final ExecutorService threadPoolSend; // Pool used to send the files in parallel.
    private final ExecutorService threadPoolReceive; // Pool used to receive the files in parallel.
    private final DFSDatabase database; // The database where the files are stored.
    
    private QuorumThread quorum_t;
    private AntiEntropyService aeService;
    
    private AtomicBoolean shutDown = new AtomicBoolean( false );
    
    private final TCPnet net;
    
    private static final int MAX_CONN = 32; // Maximum number of accepted connections.
    private static final Logger LOGGER = Logger.getLogger( FileTransferThread.class );
    private static final int PORT_OFFSET = 2;
    
    
    
    
    public FileTransferThread( GossipMember node,
                               int port,
                               ConsistentHasher<GossipMember, String> cHasher,
                               QuorumThread quorum_t,
                               String resourcesLocation,
                               String databaseLocation ) throws IOException, DFSException
    {
        setName( "FileTransfer" );
        
        database = new DFSDatabase( resourcesLocation, databaseLocation, this );
        
        aeService = new AntiEntropyService( node, this, database, cHasher );
        
        net = new TCPnet( node.getHost(), port + PORT_OFFSET );
        net.setSoTimeout( 500 );
        
        //threadPoolSend = Executors.newCachedThreadPool();
        threadPoolSend = Executors.newFixedThreadPool( MAX_CONN );
        threadPoolReceive = Executors.newFixedThreadPool( MAX_CONN );
        
        this.quorum_t = quorum_t;
    }
    
    @Override
    public void run()
    {
        try { database.newInstance(); }
        catch( IOException e1 ) { e1.printStackTrace(); }
        aeService.start();
        
        LOGGER.info( "FileTransferThread launched." );
        
        try {
            while(!shutDown.get()) {
                Session session = net.waitForConnection();
                if(session == null)
                    continue;
                
                LOGGER.info( "Received a connection from \"" + session.getEndPointAddress() + "\"" );
                synchronized( threadPoolReceive ) {
                    if(threadPoolReceive.isShutdown())
                        break;
                    
                    threadPoolReceive.execute( new ReceiveFilesThread( session ) );
                }
            }
        }
        catch( IOException e ) {
            e.printStackTrace();
        }
        
        net.close();
        
        LOGGER.info( "FileTransferThread closed." );
    }
    
    /**
     * Enable/disable the anti-entropy mechanism.<br>
     * By default this value is setted to {@code true}.
     * 
     * @param enable    {@code true} to enable the anti-entropy mechanism,
     *                  {@code false} otherwise
    */
    public void setAntiEntropy( boolean enable )
    {
        aeService.setAntiEntropy( enable );
    }
    
    @Override
    public void receiveFiles( Session session ) throws IOException
    {
        ByteBuffer data = ByteBuffer.wrap( session.receive() );
        // Read the synch attribute.
        boolean toSynch = (data.get() == (byte) 0x1);
        // Read the quorum attribute.
        boolean isQuorum = (data.get() == (byte) 0x1);
        String fileName = null;
        long fileId = 0L;
        QuorumFile qFile = null;
        if(isQuorum) {
            fileName = new String( DFSUtils.getNextBytes( data ), StandardCharsets.UTF_8 );
            fileId = data.getLong();
            qFile = quorum_t.getQuorumFile( fileName );
        }
        
        // Get the number of files.
        int numFiles = data.getInt();
        
        LOGGER.debug( "Receiving " + numFiles + " files..." );
        
        try {
            for(int i = 0; i < numFiles; i++) {
                data = ByteBuffer.wrap( session.receive( qFile ) );
                DistributedFile file = new DistributedFile( DFSUtils.getNextBytes( data ) );
                LOGGER.debug( "File \"" + file + "\" downloaded." );
                if(file.isDeleted())
                    database.deleteFile( file, file.getHintedHandoff() );
                else
                    database.saveFile( file, file.getVersion(), null, true );
            }
            
            LOGGER.debug( "Files successfully downloaded." );
            
            // Send just 1 byte for the synchronization.
            if(toSynch)
                session.sendMessage( Networking.TRUE, false );
        }
        catch( IOException e ) {
            e.printStackTrace();
        }
        
        if(isQuorum) // Release the file.
            quorum_t.unlockFile( fileName, fileId );
    }
    
    @Override
    public boolean sendFiles( String address,
                              int port,
                              List<DistributedFile> files,
                              boolean wait_response,
                              String synchNodeId,
                              QuorumNode node )
    {
        SendFilesThread t = new SendFilesThread( port + PORT_OFFSET, files, address, synchNodeId, node );
        synchronized( threadPoolSend ) {
            if(!threadPoolSend.isShutdown())
                threadPoolSend.execute( t );
            else
                return false;
        }
        
        if(!wait_response)
            return true;
        
        try{ t.join(); }
        catch( InterruptedException e ){ return false; }
        
        return t.getResult();
    }
    
    /**
     * Updates the quorum state.
     * 
     * @param node    the quorum node
    */
    private synchronized void updateQuorum( QuorumNode node )
    {
        List<QuorumNode> nodes = node.getList();
        nodes.remove( node );
    }
    
    /**
     * Returns the own database.
    */
    public DFSDatabase getDatabase() {
        return database;
    }
    
    /**
     * Closes all the opened resources.
    */
    public void shutDown()
    {
        shutDown.set( true );
        
        synchronized( threadPoolSend ) {
            threadPoolSend.shutdown();
        }
        synchronized( threadPoolReceive ) {
            threadPoolReceive.shutdown();
        }
        
        try {
            threadPoolSend.awaitTermination( 1, TimeUnit.SECONDS );
            threadPoolReceive.awaitTermination( 1, TimeUnit.SECONDS );
        } catch( InterruptedException e1 ) {
            e1.printStackTrace();
        }
        
        database.close();
        aeService.shutDown();
    }

    /**
     * Thread used to read incoming files, in an asynchronous way.
    */
    private class ReceiveFilesThread extends Thread
    {
        private Session session;
        
        public ReceiveFilesThread( Session session )
        {
            setName( "ReceiverFile" );
            
            this.session = session;
        }
        
        @Override
        public void run()
        {
            try {
                receiveFiles( session );
            }
            catch( IOException e ) {
                e.printStackTrace();
            }
            
            session.close();
        }
    }
    
    /**
     * Thread used to send files, in an asynchronously way.
    */
    private class SendFilesThread extends Thread
    {
        private int port;
        private List<DistributedFile> files;
        private String address;
        private String synchNodeId;
        private QuorumNode node;
        
        private boolean result;
        
        public SendFilesThread( int port,
                                List<DistributedFile> files,
                                String address,
                                String synchNodeId,
                                QuorumNode node )
        {
            setName( "SendFiles" );
            
            this.port = port;
            this.files = files;
            this.address = address;
            this.synchNodeId = synchNodeId;
            this.node = node;
        }
        
        @Override
        public void run()
        {
            result = true;
            Session session = null;
            
            try {
                LOGGER.debug( "Connecting to " + address + ":" + port );
                session = net.tryConnect( address, port, 2000 );
                
                int size = files.size();
                LOGGER.debug( "Sending " + size + " files to \"" + address + ":" + port + "\"" );
                
                byte[] msg = new byte[]{ (synchNodeId != null) ? (byte) 0x1 : (byte) 0x0, (node != null) ? (byte) 0x1 : (byte) 0x0 };
                if(node != null) {
                    // Add the file of the quorum to send.
                    msg = net.createMessage( msg, files.get( 0 ).getName().getBytes( StandardCharsets.UTF_8 ), true );
                    msg = net.createMessage( msg, DFSUtils.longToByteArray( node.getId() ), false );
                }
                msg = net.createMessage( msg, DFSUtils.intToByteArray( size ), false );
                session.sendMessage( msg, true );
                
                for(int i = 0; i < size; i++) {
                    DistributedFile dFile = files.get( i );
                    msg = net.createMessage( null, dFile.read(), true );
                    
                    LOGGER.debug( "Sending file \"" + dFile + "\"" );
                    session.sendMessage( msg, true );
                    LOGGER.debug( "File \"" + dFile.getName() + "\" transmitted." );
                }
                
                if(synchNodeId != null) {
                    // The destination node has received all the files.
                    // We can remove the node from the synchronized ones.
                    session.receiveMessage();
                    aeService.getReceiver().removeFromSynch( synchNodeId );
                }
                
                if(node != null)
                    updateQuorum( node );
            }
            catch( IOException e ) {
                e.printStackTrace();
                result = false;
                if(synchNodeId != null) {
                    aeService.getReceiver().removeFromSynch( synchNodeId );
                    if(node != null)
                        updateQuorum( node );
                }
            }
            
            if(session != null)
                session.close();
        }
        
        /** 
         * Returns the result of the operation.
        */
        public boolean getResult()
        {
            return result;
        }
    }
}
