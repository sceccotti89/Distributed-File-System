
package gossiping.event;

import gossiping.GossipNode;

public interface GossipListener
{
	void gossipEvent( final GossipNode member, final GossipState state );
}