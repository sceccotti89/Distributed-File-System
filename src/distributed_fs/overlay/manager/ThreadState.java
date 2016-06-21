
package distributed_fs.overlay.manager;

import java.util.Deque;

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
	private final Deque<Integer> actionsList;
	private TCPnet net;
	private TCPSession session;
	
	// TODO aggiungere i metodi e i campi per la gestione delle varie fasi del nodo
	
	public ThreadState( final long id,
						final Deque<Integer> actionsList,
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
		this.actionsList = actionsList;
	}
	
	public FileTransferThread getFileManager() { return fMgr; }
	public QuorumThread getQuorumThread(){ return quorum_t; }
	public ConsistentHasherImpl<GossipMember, String> getHashing(){ return cHasher; }
	public NetworkMonitor getNetMonitor(){ return netMonitor; }
	
	public long getId(){ return id; }
	public Deque<Integer> getActionsList(){ return actionsList; }
	public TCPnet getNet(){ return net; }
	public TCPSession getSession(){ return session; }
}