
package gossiping.manager.random;

import java.util.List;

import gossiping.GossipMember;
import gossiping.GossipSettings;
import gossiping.event.GossipListener;
import gossiping.manager.GossipManager;
import gossiping.manager.impl.OnlyProcessReceivedPassiveGossipThread;

public class RandomGossipManager extends GossipManager 
{
	public RandomGossipManager( String address, int port, String id, int virtualNodes, int nodeType,
								GossipSettings settings, List<GossipMember> gossipMembers,
								GossipListener listener ) 
	{
		super( OnlyProcessReceivedPassiveGossipThread.class, RandomActiveGossipThread.class,
			   address, port, id, virtualNodes, nodeType, settings, gossipMembers, listener );
	}
}
