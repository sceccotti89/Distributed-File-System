/**
 * @author Stefano Ceccotti
*/

package distributed_fs.overlay.manager;

import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import distributed_fs.consistent_hashing.ConsistentHasher;
import distributed_fs.exception.DFSException;
import distributed_fs.net.Networking.Session;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.net.manager.NetworkMonitorReceiverThread;
import distributed_fs.net.manager.NetworkMonitorSenderThread;
import distributed_fs.net.manager.NetworkMonitorThread;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import gossiping.GossipMember;

public class ThreadMonitor extends Thread
{
    private final DFSNode node;
    private final ExecutorService threadPool;
    private final List<Thread> threads;
    private final Map<Long, Short> restarted;
    
    private final String address;
    private final int port;
    
    private static final int MAX_RESTART = 3;
    private static final int SLEEP = 200;
    
    // ============  Used only by the StorageNode  ============ //
    private GossipMember me;
    private QuorumThread quorum_t;
    private ConsistentHasher<GossipMember, String> cHasher;
    private String resourcesLocation;
    private String databaseLocation;
    // ======================================================== //
    
    private AtomicBoolean closed = new AtomicBoolean( false );
    
    
    
    
    public ThreadMonitor( DFSNode parentNode,
                          ExecutorService threadPool,
                          List<Thread> threads,
                          String address,
                          int port,
                          int MAX_USERS )
    {
        setName( "ThreadMonitor" );
        
        this.node = parentNode;
        this.threadPool = threadPool;
        this.threads = threads;
        
        this.address = address;
        this.port = port;
        
        restarted = new HashMap<>( MAX_USERS );
    }
    
    public void addElements( GossipMember me,
                             QuorumThread quorum_t,
                             ConsistentHasher<GossipMember, String> cHasher,
                             String resourcesLocation,
                             String databaseLocation )
    {
        this.me = me;
        this.quorum_t = quorum_t;
        this.cHasher = cHasher;
        this.resourcesLocation = resourcesLocation;
        this.databaseLocation = databaseLocation;
    }
    
    @Override
    public void run()
    {
        DFSNode.LOGGER.info( "ThreadMonitor launched." );
        
        while(!closed.get()) {
            try{ sleep( SLEEP ); }
            catch( InterruptedException e1 ) { break; }
            
            try { scanThreads(); }
            catch( IOException e ) {
                e.printStackTrace();
            }
        }
        
        DFSNode.LOGGER.info( "ThreadMonitor closed." );
    }
    
    private synchronized void scanThreads() throws IOException
    {
        ListIterator<Thread> it = threads.listIterator();
        while(it.hasNext()) {
            Thread thread = it.next();
            
            if(thread.getState() == Thread.State.TERMINATED) {
                it.remove();
                
                if(!(thread instanceof DFSNode)) {
                    // Not a distributed Thread.
                    if(thread instanceof QuorumThread) {
                        QuorumThread quorum_t = new QuorumThread( port, address, node );
                        quorum_t.start();
                    }
                    else if(thread instanceof FileTransferThread) {
                        FileTransferThread fMgr;
                        try {
                            fMgr = new FileTransferThread( me, port, cHasher, quorum_t, resourcesLocation, databaseLocation );
                            fMgr.start();
                        } catch( DFSException e ) {
                            e.printStackTrace();
                        }
                    }
                    else if(thread instanceof NetworkMonitorReceiverThread){
                        NetworkMonitorReceiverThread netMonitor = new NetworkMonitorReceiverThread( address );
                        netMonitor.start();
                    }
                    else {
                        // NetworkMonitorSenderThread instance.
                        NetworkMonitorSenderThread netMonitor = new NetworkMonitorSenderThread( address, node );
                        netMonitor.start();
                    }
                }
                else {
                    DFSNode node = (DFSNode) thread;
                    if(!node.isCompleted()) {
                        // The same thread can be restarted a maximum of MAX_RESTART time.
                        Short value = restarted.get( node.getId() );
                        value = (value == null) ? 1 : value++;
                        if(value == MAX_RESTART)
                            continue;
                        restarted.put( node.getId(), value );
                        
                        // The current thread is dead due to some internal error.
                        // A new thread is started to replace this one.
                        try {
                            ThreadState state = node.getJobState();
                            if(node instanceof StorageNode)
                                node = StorageNode.startThread( threadPool, state );
                            else // LOAD BALANCER
                                node = LoadBalancer.startThread( threadPool, state );
                            
                            if(node != null)
                                it.add( node );
                        }
                        catch( IOException e ){
                            // Ignored.
                            //e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
    
    public List<Thread> getThreadsList() {
        // Used by the StorageNode (or LoadBalancer) if it crashes.
        return threads;
    }
    
    public synchronized void addThread( Thread node )
    {
        threads.add( node );
    }
    
    public void close()
    {
        closed.set( true );
        interrupt();
    }
    
    public static class ThreadState
    {
        private final FileTransferThread fMgr;
        private final QuorumThread quorum_t;
        private final ConsistentHasher<GossipMember, String> cHasher;
        private final NetworkMonitorThread netMonitor;
        
        private final long id;
        private final boolean replacedThread;
        private final Deque<Object> actionsList;
        private TCPnet net;
        private Session session;
        
        private Map<String, Object> values;
        
        public ThreadState( long id,
                            boolean replacedThread,
                            Deque<Object> actionsList,
                            FileTransferThread fMgr,
                            QuorumThread quorum_t,
                            ConsistentHasher<GossipMember, String> cHasher,
                            TCPnet net,
                            Session session,
                            NetworkMonitorThread netMonitor )
        {
            this.fMgr = fMgr;
            this.quorum_t = quorum_t;
            this.cHasher = cHasher;
            this.netMonitor = netMonitor;
            
            this.net = net;
            this.session = session;
            
            this.id = id;
            this.replacedThread = replacedThread;
            this.actionsList = actionsList;
            
            values = new HashMap<>( 16 );
        }
        
        public FileTransferThread getFileManager() { return fMgr; }
        public QuorumThread getQuorumThread(){ return quorum_t; }
        public ConsistentHasher<GossipMember, String> getHashing(){ return cHasher; }
        public NetworkMonitorThread getNetMonitor(){ return netMonitor; }
        
        public long getId(){ return id; }
        public boolean isReplacedThread() { return replacedThread; }
        public Deque<Object> getActionsList(){ return actionsList; }
        public TCPnet getNet(){ return net; }
        public Session getSession(){ return session; }
        
        @SuppressWarnings("unchecked")
        public <T> T getValue( String key ) { return (T) values.get( key ); }
        public void setValue( String key, Object value ) { if(value != null) values.put( key, value ); }
        public void removeValue( String key ) { values.remove( key ); }
        //public int getValuesSize() { return values.size(); }
        
        /* Keys used to save the objects for the recovery phase. */
        public static final String
                    NEW_MSG_REQUEST      = "A",
                    SUCCESSOR_NODES      = "B",
                    AGREED_NODES         = "C",
                    QUORUM_ERRORS        = "D",
                    AGREED_NODE_CONN     = "E",
                    QUORUM_MSG_RESPONSE  = "F",
                    RELEASE_QUORUM_CONN  = "G",
                    UPDATE_CLOCK_DB      = "H",
                    OPENED_SESSIONS      = "I",
                    REPLICA_REQUEST_CONN = "J",
                    FILES_TO_SEND        = "K",
                    REPLICA_FILE         = "L",
                    QUORUM_OFFSET        = "M",
                    NODES_INDEX          = "N",
                    GOSSIP_NODE_ID       = "O",
                    GOSSIP_NODE          = "P",
                    BALANCED_NODE_CONN   = "Q";
    }
}
