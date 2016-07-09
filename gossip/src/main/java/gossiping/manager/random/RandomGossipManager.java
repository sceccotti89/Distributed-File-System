
package gossiping.manager.random;

import java.util.List;

import gossiping.GossipMember;
import gossiping.GossipSettings;
import gossiping.event.GossipListener;
import gossiping.manager.GossipManager;
import gossiping.manager.impl.OnlyProcessReceivedPassiveGossipThread;

public class RandomGossipManager extends GossipManager 
{
	public RandomGossipManager( final String address, final int port, final String id, final int virtualNodes, final int nodeType,
								final GossipSettings settings, final List<GossipMember> gossipMembers,
								final GossipListener listener ) 
	{
		super( OnlyProcessReceivedPassiveGossipThread.class, RandomActiveGossipThread.class,
			   address, port, id, virtualNodes, nodeType, settings, gossipMembers, listener );
	}
}
