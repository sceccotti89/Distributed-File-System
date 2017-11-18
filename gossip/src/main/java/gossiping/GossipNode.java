
package gossiping;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Class used to manage a GossipMember object,
 * associating a timestamp to it.<br>
 * The timestamp could be used by the invoking service, or a remote one,
 * to select the nodes according to their up time.
*/
public class GossipNode implements Serializable, Comparable<GossipNode>
{
    private long timestamp;
    private GossipMember member;
    
    private static final long serialVersionUID = -3223285624395772477L;

    public GossipNode( GossipMember member )
    {
        timestamp = System.currentTimeMillis();
        this.member = member;
    }
    
    public void updateTimestamp() { timestamp = System.currentTimeMillis(); }
    
    public long getTimestamp() { return timestamp; }
    public GossipMember getMember() { return member; }
    
    @Override
    public int compareTo( GossipNode other )
    {
        if(timestamp > other.timestamp) return 1;
        if(timestamp < other.timestamp) return -1;
        return 0;
    }

    @Override
    public boolean equals( Object obj )
    {
        if(!(obj instanceof GossipNode)) {
            System.err.println( "equals(): obj is not of type GossipNode." );
            return false;
        }
        
        GossipNode other = (GossipNode) obj;
        return member.equals( other.getMember() );
    }
    
    @Override
    public String toString()
    {
        return "{ts = " + timestamp + ", " + member.toString() + "}";
    }
    
    /**
     * Class used to compare two GossipNode.
    */
    public static class CompareNodes implements Comparator<GossipNode>
    {
        @Override
        public int compare( GossipNode o1, GossipNode o2 )
        {
            return o1.getMember().compareTo( o2.getMember() );
        }
    }
}
