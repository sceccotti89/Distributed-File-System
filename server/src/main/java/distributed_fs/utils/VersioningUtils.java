/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package distributed_fs.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import distributed_fs.storage.DistributedFile;
import distributed_fs.versioning.ClockEntry;
import distributed_fs.versioning.Occurred;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Versioned;

/**
 * Helper functions FTW!
 * 
 * 
 */
public class VersioningUtils {

    public static final String NEWLINE = System.getProperty( "line.separator" );

    /**
     * Pattern for splitting a string based on commas
     */
    public static final Pattern COMMA_SEP = Pattern.compile( "\\s*,\\s*" );
    
    /**
     * Compare two VectorClocks, the outcomes will be one of the following: <br>
     * -- Clock 1 is BEFORE clock 2, if there exists an nodeId such that
     * c1(nodeId) <= c2(nodeId) and there does not exist another nodeId such
     * that c1(nodeId) > c2(nodeId). <br>
     * -- Clock 1 is CONCURRENT to clock 2 if there exists an nodeId, nodeId2
     * such that c1(nodeId) < c2(nodeId) and c1(nodeId2) > c2(nodeId2)<br>
     * -- Clock 1 is AFTER clock 2 otherwise
     *
     * @param v1 The first VectorClock
     * @param v2 The second VectorClock
     */
    public static Occurred compare( VectorClock v1, VectorClock v2 ) 
    {
        if(v1 == null || v2 == null)
            throw new IllegalArgumentException( "Can't compare null vector clocks!" );
        
        // We do two checks: v1 <= v2 and v2 <= v1 if both are true then
        boolean v1Bigger = false;
        boolean v2Bigger = false;
        
        SortedSet<String> v1Nodes = v1.getVersionMap().navigableKeySet();
        SortedSet<String> v2Nodes = v2.getVersionMap().navigableKeySet();
        // get clocks(nodeIds) that both v1 and v2 has
        SortedSet<String> commonNodes = new TreeSet<>( v1Nodes );
        commonNodes.retainAll( v2Nodes );
        // if v1 has more nodes than common nodes
        // v1 has clocks that v2 does not
        if (v1Nodes.size() > commonNodes.size()) {
            v1Bigger = true;
        }
        // if v2 has more nodes than common nodes
        // v2 has clocks that v1 does not
        if (v2Nodes.size() > commonNodes.size()) {
            v2Bigger = true;
        }
        // compare the common parts
        for (String nodeId: commonNodes) {
            // no need to compare more: they are concurrently
            if(v1Bigger && v2Bigger) {
                break;
            }
            long v1Version = v1.getVersionMap().get( nodeId );
            long v2Version = v2.getVersionMap().get( nodeId );
            if (v1Version > v2Version) {
                v1Bigger = true;
            } else if (v1Version < v2Version) {
                v2Bigger = true;
            }
        }

        /* This is the case where they are equal. */
        if(!v1Bigger && !v2Bigger)
            return Occurred.AFTER;
        /* This is the case where v1 is a successor clock to v2. */
        else if(v1Bigger && !v2Bigger)
            return Occurred.AFTER;
        /* This is the case where v2 is a successor clock to v1. */
        else if(!v1Bigger && v2Bigger)
            return Occurred.BEFORE;
        /* This is the case where both clocks are parallel to one another. */
        else
            return Occurred.CONCURRENTLY;
    }

    /**
     * Given a set of versions, constructs a resolved list of versions based on
     * the compare function above.<br>
     * The returned list doesn't contain obsolete items.
     *
     * @param values
     * 
     * @return list of values after resolution
    */
    public static <T> List<Versioned<T>> resolveVersions( List<Versioned<T>> values ) 
    {
        List<Versioned<T>> resolvedVersions = new ArrayList<Versioned<T>>( values.size() );
        // Go over all the values and determine whether the version is
        // acceptable.
        for(Versioned<T> value: values) {
            ListIterator<Versioned<T>> iter = resolvedVersions.listIterator();
            boolean obsolete = false;
            //System.out.println( "VALUE: " + value + ", RESOLVED: " + resolvedVersions );
            // Compare the current version with a set of accepted versions
            while(iter.hasNext()) {
                Versioned<T> curr = iter.next();
                Occurred occurred = value.getVersion().compare( curr.getVersion() );
                //System.out.println( "VALUE: " + value + ", CURR: " + curr + ", COMPARE: " + occurred );
                if(occurred == Occurred.BEFORE || occurred == Occurred.EQUALS) {
                    obsolete = true;
                    break;
                } else if (occurred == Occurred.AFTER) {
                    iter.remove();
                }
            }
            if (!obsolete) {
                // else update the set of accepted versions
                resolvedVersions.add(value);
            }
        }
        
        return resolvedVersions;
    }
    
    /**
     * Makes the reconciliation among different vector clocks.
     * 
     * @param files     list of files to compare
     * 
     * @return The list of uncorrelated versions.
    */
    public static List<DistributedFile> makeReconciliation( List<DistributedFile> files )
    {
        List<Versioned<DistributedFile>> versions = new ArrayList<>();
        for(DistributedFile file : files)
            versions.add( new Versioned<DistributedFile>( file, file.getVersion() ) );
        
        // Resolve the versions..
        List<Versioned<DistributedFile>> inconsistency = VersioningUtils.resolveVersions( versions );
        
        // Get the uncorrelated files.
        List<DistributedFile> uncorrelatedVersions = new ArrayList<>();
        for(Versioned<DistributedFile> version : inconsistency)
            uncorrelatedVersions.add( version.getValue() );
        
        return uncorrelatedVersions;
    }

    /**
     * Generates a vector clock with the provided values.
     *
     * @param serverIds        servers in the clock
     * @param clockValue    value of the clock for each server entry
     * @param timestamp        ts value to be set for the clock
     * 
     * @return
    */
    @SuppressWarnings("deprecation")
    public static VectorClock makeClock(Set<String> serverIds, long clockValue, long timestamp) 
    {
        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>(serverIds.size());
        for (String serverId: serverIds) {
            clockEntries.add( new ClockEntry( serverId, clockValue ) );
        }
        return new VectorClock(clockEntries, timestamp);
    }

    /**
     * Generates a vector clock with the provided nodes and current time stamp
     * This clock can be used to overwrite the existing value avoiding obsolete
     * version exceptions in most cases, except If the existing Vector Clock was
     * generated in custom way. (i.e. existing vector clock does not use
     * milliseconds)
     * 
     * @param serverIds servers in the clock
    */
    public static VectorClock makeClockWithCurrentTime( Set<String> serverIds ) 
    {
        return makeClock( serverIds, System.currentTimeMillis(), System.currentTimeMillis() );
    }
    
    /**
     * Determines if two objects are equal as determined by
     * {@link Object#equals(Object)}, or "deeply equal" if both are arrays.
     * <p>
     * If both objects are null, true is returned; if both objects are array,
     * the corresponding {@link Arrays#deepEquals(Object[], Object[])}, or
     * {@link Arrays#equals(int[], int[])} or the like are called to determine
     * equality.
     * <p>
     * Note that this method does not "deeply" compare the fields of the
     * objects.
    */
    public static boolean deepEquals( Object o1, Object o2 )
    {
        if(o1 == o2) {
            return true;
        }
        if(o1 == null || o2 == null) {
            return false;
        }

        Class<?> type1 = o1.getClass();
        Class<?> type2 = o2.getClass();
        if(!(type1.isArray() && type2.isArray())) {
            return o1.equals(o2);
        }
        if(o1 instanceof Object[] && o2 instanceof Object[]) {
            return Arrays.deepEquals((Object[]) o1, (Object[]) o2);
        }
        if(type1 != type2) {
            return false;
        }
        if(o1 instanceof boolean[]) {
            return Arrays.equals((boolean[]) o1, (boolean[]) o2);
        }
        if(o1 instanceof char[]) {
            return Arrays.equals((char[]) o1, (char[]) o2);
        }
        if(o1 instanceof byte[]) {
            return Arrays.equals((byte[]) o1, (byte[]) o2);
        }
        if(o1 instanceof short[]) {
            return Arrays.equals((short[]) o1, (short[]) o2);
        }
        if(o1 instanceof int[]) {
            return Arrays.equals((int[]) o1, (int[]) o2);
        }
        if(o1 instanceof long[]) {
            return Arrays.equals((long[]) o1, (long[]) o2);
        }
        if(o1 instanceof float[]) {
            return Arrays.equals((float[]) o1, (float[]) o2);
        }
        if(o1 instanceof double[]) {
            return Arrays.equals((double[]) o1, (double[]) o2);
        }
        throw new AssertionError();
    }
}
