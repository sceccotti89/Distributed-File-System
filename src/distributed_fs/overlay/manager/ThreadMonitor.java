package distributed_fs.overlay.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;

import org.json.JSONException;

import distributed_fs.overlay.DFSNode;
import distributed_fs.overlay.StorageNode;
import gossiping.GossipMember;

public class ThreadMonitor extends Thread
{
	private final ExecutorService threadPool;
	
	private List<DFSNode> threads;
	
	public ThreadMonitor( final ExecutorService threadPool )
	{
		this.threadPool = threadPool;
		threads = new ArrayList<>( DFSNode.MAX_USERS );
		
		setDaemon( true );
	}
	
	@Override
	public void run()
	{
		while(true) {
			try{ sleep( 200 ); }
			catch( InterruptedException e1 ) { break; }
			
			// TODO mutua esclusione con l'addThread??
			ListIterator<DFSNode> it = threads.listIterator();
			while(it.hasNext()) {
				DFSNode node = it.next();
				
				if(node.getState() == Thread.State.TERMINATED) {
					if(node.isCompleted())
						it.remove();
					else {
						// The current thread is dead due to some internal error.
						// A new thread is started to replace this one.
						try {
							ThreadState state = node.getJobState();
							if(node.getNodeType() == GossipMember.STORAGE)
								node = StorageNode.startThread( threadPool, state );
							else // LOAD BALANCER
								;//LoadBalancer.startThread( node.getId(), node.getActionsList() );
							
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
	
	public void addThread( final DFSNode node )
	{
		// TODO sincronizzare
		threads.add( node );
	}
}