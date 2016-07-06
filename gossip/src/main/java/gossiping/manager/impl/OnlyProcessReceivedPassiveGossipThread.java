package gossiping.manager.impl;

import java.util.List;

import gossiping.GossipMember;
import gossiping.GossipNode;
import gossiping.GossipService;
import gossiping.LocalGossipMember;
import gossiping.RemoteGossipMember;
import gossiping.event.GossipState;
import gossiping.manager.GossipManager;
import gossiping.manager.PassiveGossipThread;

public class OnlyProcessReceivedPassiveGossipThread extends PassiveGossipThread 
{
	public OnlyProcessReceivedPassiveGossipThread( final GossipManager gossipManager ) 
	{
		super( gossipManager );
	}

  /**
   * Merge remote list (received from peer), and our local member list. Simply, we must update the
   * heartbeats that the remote list has with our list. Also, some additional logic is needed to
   * make sure we have not timed out a member and then immediately received a list with that member.
   *
   * @param gossipManager
   * @param senderMember
   * @param remoteList
   */
	@Override
	protected void mergeLists( final GossipManager gossipManager, 
							   final RemoteGossipMember senderMember,
		  					   final List<GossipMember> remoteList ) 
	{
		List<GossipNode> deadList = gossipManager.getDeadList();
		List<GossipNode> memberList = gossipManager.getMemberList();
		int timeInterval = gossipManager.getSettings().getTimeIntervals();
		
		for(GossipMember remoteMember : remoteList) {
			// Skip myself. We don't want ourselves in the local member list.
			if (remoteMember.equals( gossipManager.getMyself() )) {
				continue;
			}
			
			// Skip the nodes with virtual nodes equals to 0. They are not yet initialized.
			if(remoteMember.getVirtualNodes() == 0) {
				continue;
			}
			
			int index = memberList.indexOf( remoteMember );
			if(index >= 0) {
				LocalGossipMember localMember = (LocalGossipMember) memberList.get( index ).getMember();
				// Check whether the node has been updated.
				if(!localMember.isUpdated()) {
					localMember.setUpdated( true );
					localMember.setVirtualNodes( remoteMember.getVirtualNodes() );
					gossipManager.createOrRevivieMember( localMember );
				}
				
				if (remoteMember.getHeartbeat() > localMember.getHeartbeat()) {
					localMember.setHeartbeat( remoteMember.getHeartbeat() );
					localMember.resetTimeoutTimer();
					gossipManager.updateMember( localMember, GossipState.UP );
				}
			} else {
				// The remote member is either brand new, or a previously declared dead member.
				// If its dead, check the heartbeat because it may have come back from the dead.
			    index = deadList.indexOf( remoteMember );
				if(index >= 0) {
					// The remote member is known here as a dead member.
					GossipService.LOGGER.debug( "The remote member is known here as a dead member." );
					int localDeadHeartbeat = ((LocalGossipMember) deadList.get( index ).getMember()).getHeartbeat();
					// If a member is restarted the heartbeat will restart from 1, so we should check that here.
					// So a member can become from the dead when it is either larger than a previous heartbeat (due to network failure)
					// or when the heartbeat is 1 (after a restart of the service).
					// TODO: What if the first message of a gossip service is sent to a dead node? The second member will receive a heartbeat of two.
					// TODO: The above does happen. Maybe a special message for a revived member?
					// TODO: Or maybe when a member is declared dead for more than _settings.getCleanupInterval() ms, reset the heartbeat to 0.
					// It will then accept a revived member.
					// The above is now handle by checking whether the heartbeat differs _settings.getCleanupInterval(), it must be restarted.
					int remoteHeartbeat = remoteMember.getHeartbeat();
					if(remoteHeartbeat == 1 ||
					  (remoteHeartbeat - localDeadHeartbeat) > timeInterval ||
					  remoteHeartbeat > localDeadHeartbeat) {
						GossipService.LOGGER.debug( "The remote member is back from the dead. We will remove it from the dead list and add it as a new member." );
						// The remote member is back from the dead.
						// Add it as a new member and add it to the member list.
						LocalGossipMember newLocalMember =
								new LocalGossipMember( remoteMember.getHost(), remoteMember.getPort(), remoteMember.getId(),
													   remoteMember.getVirtualNodes(), remoteMember.getNodeType(), remoteMember.getHeartbeat(),
													   true, gossipManager, gossipManager.getSettings().getCleanupInterval() );
						// gossipManager.getMemberList().add(newLocalMember);
						gossipManager.createOrRevivieMember( newLocalMember );
						newLocalMember.startTimeoutTimer();
						GossipService.LOGGER.debug( "Removed remote member " + remoteMember.getAddress() + " from dead list and added to local member list." );
					}
				} else {
					// Brand spanking new member - welcome.
					LocalGossipMember newLocalMember =
							new LocalGossipMember( remoteMember.getHost(), remoteMember.getPort(), remoteMember.getId(),
												   remoteMember.getVirtualNodes(), remoteMember.getNodeType(), remoteMember.getHeartbeat(),
												   true, gossipManager, gossipManager.getSettings().getCleanupInterval() );
					gossipManager.createOrRevivieMember( newLocalMember );
					newLocalMember.startTimeoutTimer();
					GossipService.LOGGER.info( "Added new remote member " + remoteMember.getAddress() + " to local member list." );
				}
			}
		}
	}
}