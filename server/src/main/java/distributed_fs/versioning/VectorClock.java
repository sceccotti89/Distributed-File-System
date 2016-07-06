/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package distributed_fs.versioning;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import distributed_fs.utils.Utils;
import distributed_fs.utils.VersioningUtils;

/**
 * A vector of the number of writes mastered by each node. The vector is stored
 * sparely, since, in general, writes will be mastered by only one node. This
 * means implicitly all the versions are at zero, but we only actually store
 * those greater than zero.
*/
public class VectorClock implements Version, Serializable
{
    private static final long serialVersionUID = 1;

    private static final long MAX_NUMBER_OF_VERSIONS = 10;

    /** A map of versions keyed by nodeId */
    private TreeMap<String, Long> versionMap;
    /** A map of timestamps keyed by nodeId */
    private TreeMap<String, Long> timestampMap;

    /** The time of the last update on the server on which the update was performed */
    private volatile long timestamp;

    /**
     * Construct an empty VectorClock.
     * Used only inside a versioned object.
    */
    public VectorClock() 
    {
        this( System.currentTimeMillis() );
    }
    
    /*public VectorClock( final byte[] data )
    {
    	write( data );
    }*/
    
    public VectorClock( final long timestamp ) 
    {
        this.versionMap = new TreeMap<String, Long>();
        this.timestampMap = new TreeMap<String, Long>();
        this.timestamp = timestamp;
    }
    
    /**
     * This function is not safe because it may break the pre-condition that
     * clock entries should be sorted by nodeId 
     */
    @Deprecated
    public VectorClock( final List<ClockEntry> versions, final long timestamp ) 
    {
        this.versionMap = new TreeMap<String, Long>();
        this.timestampMap = new TreeMap<String, Long>();
        this.timestamp = timestamp;
        for(ClockEntry clockEntry: versions) {
        	this.versionMap.put( clockEntry.getNodeId(), clockEntry.getVersion() );
        }
    }
    
    /**
     * Only used for cloning
     * 
     * @param versionMap
     * @param timestampMap
     * @param timestamp
     */
    private VectorClock( final TreeMap<String, Long> versionMap, final TreeMap<String, Long> timestampMap,
    					 final long timestamp ) 
    {
        this.versionMap = Utils.checkNotNull( versionMap );
        this.timestampMap = Utils.checkNotNull( timestampMap );
        this.timestamp = timestamp;
    }
    
    /*public VectorClock( final byte[] data )
	{
		write( data );
	}*/
	
	/**
     * Increment the version info associated with the given node
     * 
     * @param node The node
     */
    public void incrementVersion( final String node ) 
    {
        if(node == null)
            throw new IllegalArgumentException( node + " id can't be null." );
        
        long time = System.currentTimeMillis();
        
        if(versionMap.size() == MAX_NUMBER_OF_VERSIONS) {
        	// remove the oldest version based on the timestamp
        	long min_ts = -1;
        	String min_key = null;
        	for(String key : timestampMap.keySet()) {
        		long timestamp = timestampMap.get( key );
        		if(min_ts == -1 || timestamp < min_ts) {
        			min_ts = timestamp;
        			min_key = key;
        		}
        	}
        	
        	if(min_key != null) {
        		versionMap.remove( min_key );
        		timestampMap.remove( min_key );
        	}
        }
        
        this.timestamp = time;
        
        Long version = versionMap.get( node );
        if(version == null)
            version = 1L;
        else
            version = version + 1L;
        
        timestampMap.put( node, time );
        versionMap.put( node, version );
    }
    
    /**
     * Get new vector clock based on this clock but incremented on index nodeId
     * 
     * @param nodeId The id of the node to increment
     * 
     * @return A vector clock equal on each element except that indexed by nodeId
     */
    public VectorClock incremented( final String nodeId ) 
    {
    	VectorClock copyClock = this.clone();
    	copyClock.incrementVersion( nodeId );
        return copyClock;
    }
	
	public TreeMap<String, Long> getVersionMap() 
	{
	    return versionMap;
	}
	
	@Override
    public VectorClock clone() 
    {
        return new VectorClock( new TreeMap<>( versionMap ),
        						new TreeMap<>( timestampMap ),
        						this.timestamp );
    }
	
    @Override
    public boolean equals( final Object object )
    {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (!object.getClass().equals( VectorClock.class ))
            return false;
        VectorClock clock = (VectorClock) object;
        return versionMap.equals( clock.versionMap );
    }
    
    @Override
    public int hashCode() 
    {
        return versionMap.hashCode();
    }
    
    @Override
    public String toString() 
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "version(" );
        int versionsLeft = versionMap.size();
        for(Entry<String, Long> entry: versionMap.entrySet()) {
            versionsLeft--;
            String node = entry.getKey();
            Long version = entry.getValue();
            builder.append( node + ":" + version );
            if(versionsLeft > 0) {
                builder.append( ", " );
            }
        }
        builder.append( ")" );
        builder.append( " ts:" + timestamp );
        return builder.toString();
    }
    
    public long getMaxVersion() 
    {
        long max = -1;
        for(Long version: versionMap.values())
            max = Math.max( version, max );
        return max;
    }
    
    public VectorClock merge( final VectorClock clock ) 
    {
        VectorClock newClock = new VectorClock();
        
        for(Map.Entry<String, Long> entry: this.versionMap.entrySet()) {
            newClock.versionMap.put( entry.getKey(), entry.getValue() );
        }
        
        for(Map.Entry<String, Long> entry: clock.versionMap.entrySet()) {
            Long version = newClock.versionMap.get( entry.getKey() );
            if(version == null) {
                newClock.versionMap.put( entry.getKey(), entry.getValue() );
            } else {
                newClock.versionMap.put( entry.getKey(), Math.max( version, entry.getValue() ) );
            }
        }
        
        return newClock;
    }
    
    @Override
    public Occurred compare( final Version v )
    {
        if(!(v instanceof VectorClock))
            throw new IllegalArgumentException( "Cannot compare Versions of different types." );

        return VersioningUtils.compare( this, (VectorClock) v );
    }
    
    public long getTimestamp()
    {
        return this.timestamp;
    }
}
