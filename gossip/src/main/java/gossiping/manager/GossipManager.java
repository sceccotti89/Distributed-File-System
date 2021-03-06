
package gossiping.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.log4j.Logger;

import gossiping.GossipMember;
import gossiping.GossipNode;
import gossiping.GossipNode.CompareNodes;
import gossiping.GossipService;
import gossiping.GossipSettings;
import gossiping.LocalGossipMember;
import gossiping.event.GossipListener;
import gossiping.event.GossipState;

public abstract class GossipManager extends Thread implements NotificationListener 
{
	public static final Logger LOGGER = Logger.getLogger( GossipManager.class );
	public static final int MAX_PACKET_SIZE = 102400;
	public static final int GOSSIPING_PORT = 2000;
	
	private final ConcurrentSkipListMap<GossipNode, GossipState> members;
	private final LocalGossipMember _me;
	private int vNodes;
	private final GossipSettings _settings;
	private final Class<? extends PassiveGossipThread> _passiveGossipThreadClass;
	private final Class<? extends ActiveGossipThread> _activeGossipThreadClass;
	private final GossipListener listener;
	private ActiveGossipThread activeGossipThread;
	private PassiveGossipThread passiveGossipThread;
	private ExecutorService _gossipThreadExecutor;
	
    private boolean started = false;
    private boolean updateVNodes = false;
    
    private final ReentrantLock LOCK = new ReentrantLock();
    
    
    
	
	public GossipManager( Class<? extends PassiveGossipThread> passiveGossipThreadClass,
						  Class<? extends ActiveGossipThread>  activeGossipThreadClass, 
						  final String address, 
						  final int port, 
						  final String id,
						  int virtualNodes,
						  int nodeType,
						  GossipSettings settings, 
						  final List<GossipMember> gossipMembers, 
						  final GossipListener listener ) 
	{
		_passiveGossipThreadClass = passiveGossipThreadClass;
		_activeGossipThreadClass = activeGossipThreadClass;
		_settings = settings;
		this.listener = listener;
		_me = new LocalGossipMember( address, port, id, virtualNodes, nodeType, 0, this, settings.getCleanupInterval() );
		members = new ConcurrentSkipListMap<>( new CompareNodes() );
		for(GossipMember startupMember : gossipMembers) {
			if(!startupMember.equals( _me )) {
				LocalGossipMember member = new LocalGossipMember( startupMember.getHost(), startupMember.getPort(), startupMember.getId(),
																  0, startupMember.getNodeType(), 0, this, settings.getCleanupInterval() );
				members.put( new GossipNode( member ), GossipState.UP );
				GossipService.LOGGER.debug( member );
			}
		}
		
		vNodes = virtualNodes;
		if(vNodes <= 0) {
		    updateVNodes = true;
		    updateVirtualNodes();
		}
	}

	/**
	 * All timers associated with a member will trigger this method when it goes
	 * off. The timer will go off if we have not heard from this member in
	 * <code>_settings.getCleanupInterval()</code> time.
	 */
	@Override
	public void handleNotification( Notification notification, Object handback ) 
	{
		LocalGossipMember deadMember = (LocalGossipMember) notification.getUserData();
		
		LOCK.lock();
		
		GossipService.LOGGER.info( "Dead member detected: " + deadMember );
		GossipNode node = new GossipNode( deadMember );
		members.put( node, GossipState.DOWN );
		
		// Avoid the notification of LoadBalancer nodes.
		if(deadMember.getNodeType() != GossipMember.LOAD_BALANCER &&
		   deadMember.getVirtualNodes() > 0 && listener != null) {
		    updateVirtualNodes();
			listener.gossipEvent( deadMember, GossipState.DOWN );
		}
		
		LOCK.unlock();
	}

	public void createOrRevivieMember( LocalGossipMember member )
	{
	    GossipNode node = new GossipNode( member );
	    
	    LOCK.lock();
	    
	    members.put( node, GossipState.UP );
		
		// Avoid the notification of LoadBalancer nodes.
		if(member.getNodeType() != GossipMember.LOAD_BALANCER && listener != null) {
		    updateVirtualNodes();
			listener.gossipEvent( member, GossipState.UP );
		}
		
		LOCK.unlock();
	}
	
	/**
	 * Recomputes the number of virtual nodes
	 * associated to this member.
	*/
	private void updateVirtualNodes()
	{
	    if(updateVNodes) {
	        // Get the number of storage nodes only.
    	    int size = 1;
    	    for(GossipNode node : getMemberList()) {
    	        if(node.getMember().getNodeType() == GossipMember.STORAGE && 
	               node.getMember().getVirtualNodes() > 0)
    	            size++;
    	    }
    	    
    	    vNodes = Math.max( 1, (int) Math.ceil( (Math.log( size ) / Math.log( 2 )) ) );
            _me.setVirtualNodes( vNodes );
	    }
	}
	
	public int getVirtualNodes()
	{
	    return vNodes;
	}
	
	public void removeMember( GossipMember member )
	{
	    GossipNode node = new GossipNode( member );
	    LOCK.lock();
        members.put( node, GossipState.DOWN );
        LOCK.unlock();
	}
	
	public void addMember( GossipMember member )
    {
	    GossipNode node = new GossipNode( member );
	    LOCK.lock();
        members.put( node, GossipState.UP );
        LOCK.unlock();
    }

	public GossipSettings getSettings() 
	{
		return _settings;
	}

	public List<GossipNode> getMemberList() 
	{
		List<GossipNode> up = new ArrayList<>();
		for(Entry<GossipNode, GossipState> entry : members.entrySet())
			if(GossipState.UP.equals( entry.getValue() ))
				up.add( entry.getKey() );
		
		return Collections.unmodifiableList( up );
	}

	public LocalGossipMember getMyself() 
	{
		return _me;
	}

	public List<GossipNode> getDeadList() 
	{
		List<GossipNode> down = new ArrayList<>();
		for (Entry<GossipNode, GossipState> entry : members.entrySet())
			if (GossipState.DOWN.equals( entry.getValue() ))
				down.add( entry.getKey() );
		
		return Collections.unmodifiableList( down );
	}
	
	public void addShoutDownHook()
	{
	    Runtime.getRuntime().addShutdownHook( new Thread( new Runnable() 
        {
            @Override
            public void run() 
            {
                GossipService.LOGGER.info( "Service has been shutdown..." );
            }
        }));
	}
	
	public boolean isStarted()
    {
        return started;
    }

	/**
	 * Starts the client. Specifically, start the various cycles for this
	 * protocol. Start the gossip thread and start the receiver thread.
	 */
	@Override
	public void run() 
	{
	    started = true;
	    
		for(GossipNode node : members.keySet()) {
			LocalGossipMember member = (LocalGossipMember) node.getMember();
		    if(member != _me)
				member.startTimeoutTimer();
		}

		_gossipThreadExecutor = Executors.newCachedThreadPool();
		try{
			passiveGossipThread = _passiveGossipThreadClass.getConstructor( GossipManager.class ).newInstance( this );
			_gossipThreadExecutor.execute( passiveGossipThread );
			activeGossipThread = _activeGossipThreadClass.getConstructor( GossipManager.class ).newInstance( this );
			_gossipThreadExecutor.execute( activeGossipThread );
		}catch ( Exception e1 ) {
			throw new RuntimeException( e1 );
		}
		GossipService.LOGGER.info( "The GossipService is started." );
	}

	/**
	 * Shutdown the gossip service.
	 */
	public void shutdown()
	{
		_gossipThreadExecutor.shutdown();
		passiveGossipThread.shutdown();
		activeGossipThread.shutdown();
		try {
			boolean result = _gossipThreadExecutor.awaitTermination( 1, TimeUnit.SECONDS );
			if (!result) {
				LOGGER.error( "executor shutdown timed out" );
			}
		} catch (InterruptedException e) {
			LOGGER.error( e );
		}
	}
}
