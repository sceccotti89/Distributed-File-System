package distributed_fs.overlay.manager;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;

import org.json.JSONException;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.exception.DFSException;
import distributed_fs.net.NetworkMonitorReceiverThread;
import distributed_fs.net.NetworkMonitorSenderThread;
import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.LoadBalancer;
import distributed_fs.overlay.StorageNode;
import distributed_fs.storage.FileTransferThread;
import gossiping.GossipMember;

public class ThreadMonitor extends Thread
{
    private final DFSNode node;
	private final ExecutorService threadPool;
	private final List<Thread> threads;
	
	private final String address;
	private final int port;
	
	// ============  Used only by the StorageNode  ============ //
	private GossipMember me;
    private QuorumThread quorum_t;
    private ConsistentHasherImpl<GossipMember, String> cHasher;
    private String resourcesLocation;
    private String databaseLocation;
    // ======================================================== //
	
	private boolean closed = false;
	
	public ThreadMonitor( final DFSNode parentNode,
	                      final ExecutorService threadPool,
	                      final List<Thread> threads,
	                      final String address,
	                      final int port )
	{
	    this.node = parentNode;
		this.threadPool = threadPool;
		this.threads = threads;
		
		this.address = address;
		this.port = port;
	}
	
	public void addElements( final GossipMember me,
                             final QuorumThread quorum_t,
                             final ConsistentHasherImpl<GossipMember, String> cHasher,
                             final String resourcesLocation,
                             final String databaseLocation )
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
		while(!closed) {
			try{ sleep( 200 ); }
			catch( InterruptedException e1 ) { break; }
			
			try { scanThreads(); }
			catch( IOException e ) {
                e.printStackTrace();
            }
		}
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
                            fMgr = new FileTransferThread( me, port + 1, cHasher, quorum_t, resourcesLocation, databaseLocation );
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
                    // TODO lo stesso thread viene riavviato fino a un massimo di 3 volte
                    if(!node.isCompleted()) {
                        // The current thread is dead due to some internal error.
                        // A new thread is started to replace this one.
                        try {
                            ThreadState state = node.getJobState();
                            if(node instanceof StorageNode)
                                node = StorageNode.startThread( threadPool, state );
                            else // LOAD BALANCER
                                LoadBalancer.startThread( threadPool, state );
                            
                            if(node != null)
                                it.add( node );
                        }
                        catch( JSONException | IOException e ){
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
	
	public synchronized void addThread( final Thread node )
	{
		threads.add( node );
	}
	
	public void close()
	{
	    closed = true;
	    interrupt();
	}
}