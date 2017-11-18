package gossiping.manager.random;

import java.util.List;
import java.util.Random;

import gossiping.GossipNode;
import gossiping.GossipService;
import gossiping.LocalGossipMember;
import gossiping.manager.GossipManager;
import gossiping.manager.impl.SendMembersActiveGossipThread;

public class RandomActiveGossipThread extends SendMembersActiveGossipThread 
{
	/** The Random used for choosing a member to gossip with. */
	private final Random random;

	public RandomActiveGossipThread( GossipManager gossipManager ) 
	{
		super( gossipManager );
		random = new Random();
	}

	/**
	 * [The selectToSend() function.] Find a random peer from the local
	 * membership list. In the case where this client is the only member in the
	 * list, this method will return null.
	 *
	 * @return Member random member if list is greater than 1, null otherwise
	 */
	protected LocalGossipMember selectPartner( List<GossipNode> memberList ) 
	{
		LocalGossipMember member = null;
		if (memberList.size() > 0) {
			int randomNeighborIndex = random.nextInt( memberList.size() );
			member = (LocalGossipMember) memberList.get( randomNeighborIndex ).getMember();
		} else {
			GossipService.LOGGER.debug( "I am alone in this world." );
		}
		
		return member;
	}

}
