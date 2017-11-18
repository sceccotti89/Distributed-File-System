
package gossiping.event;

import gossiping.GossipMember;

public interface GossipListener
{
	void gossipEvent( GossipMember member, GossipState state );
}
