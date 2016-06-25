
package distributed_fs.overlay.manager;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.NetworkMonitor;
import distributed_fs.net.Networking.TCPSession;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.storage.FileTransferThread;
import gossiping.GossipMember;

public class ThreadState
{
	private final FileTransferThread fMgr;
	private final QuorumThread quorum_t;
	private final ConsistentHasherImpl<GossipMember, String> cHasher;
	private final NetworkMonitor netMonitor;
	
	private final long id;
	private final boolean replacedThread;
	private final Deque<Object> actionsList;
	private TCPnet net;
	private TCPSession session;
	
	private Map<String, Object> values;
	
	public ThreadState( final long id,
	                    final boolean replacedThread,
						final Deque<Object> actionsList,
						final FileTransferThread fMgr,
						final QuorumThread quorum_t,
						final ConsistentHasherImpl<GossipMember, String> cHasher,
						final NetworkMonitor netMonitor )
	{
		this.fMgr = fMgr;
		this.quorum_t = quorum_t;
		this.cHasher = cHasher;
		this.netMonitor = netMonitor;
		
		this.id = id;
		this.replacedThread = replacedThread;
		this.actionsList = actionsList;
		
		values = new HashMap<>( 8 );//TODO per adesso e' 8 poi vediamo (basta contare i messaggi)
	}
	
	public FileTransferThread getFileManager() { return fMgr; }
	public QuorumThread getQuorumThread(){ return quorum_t; }
	public ConsistentHasherImpl<GossipMember, String> getHashing(){ return cHasher; }
	public NetworkMonitor getNetMonitor(){ return netMonitor; }
	
	public long getId(){ return id; }
	public boolean isReplacedThread() { return replacedThread; }
	public Deque<Object> getActionsList(){ return actionsList; }
	public TCPnet getNet(){ return net; }
	public TCPSession getSession(){ return session; }
	
	@SuppressWarnings("unchecked")
    public <T> T getValue( final String key ) { return (T) values.get( key ); }
	public void setValue( final String key, final Object value ) { values.put( key, value ); }
	
	/* Keys used to save the objects for the recovery phase. */
	public static final String
	            MSG_FROM_CLIENT = "A",
	            SUCCESSOR_NODES = "B",
	            AGREED_NODES    = "C",
	            SUCC_NODE_INDEX = "D";
}