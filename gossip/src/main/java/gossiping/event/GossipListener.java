
package gossiping.event;

import gossiping.GossipMember;

public interface GossipListener
{
	void gossipEvent( final GossipMember member, final GossipState state );
}