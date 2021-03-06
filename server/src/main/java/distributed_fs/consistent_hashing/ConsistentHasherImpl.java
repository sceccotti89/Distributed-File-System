
package distributed_fs.consistent_hashing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;

import distributed_fs.utils.DFSUtils;
import gossiping.GossipMember;

/**
 * Implementation of {@link ConsistentHasher}.
 * <br>
 * The {@link #addBucket(Object)}, {@link #addMember(Object)} and
 * {@link #removeMember(Object)} operations do not use any lock, should be
 * executed instantly.
 * <br>
 * The {@link #removeBucket(Object)},
 * {@link #tryRemoveBucket(Object, long, TimeUnit)},
 * {@link #getAllBucketsToMembersMapping()} and {@link #getMembersFor(Object)} operations
 * are using a lock.
 * 
 * @param <GossipMember>    bucket type.
 * @param <M>                member type.
 */
public class ConsistentHasherImpl<B extends GossipMember, M extends Serializable> implements ConsistentHasher<B, M> 
{
    private final NavigableMap<String, B> bucketsMap;
    private final NavigableMap<String, M> membersMap;
    
    private final ConcurrentMap<B, BucketInfo> bucketsAndLocks;
    
    /**
     * Contains a lock for the bucket, and also contains all virtual bucket names.
     * 
     * Lock is used to achieve atomicity while doing operations on the bucket.
     *
    */
    private static class BucketInfo 
    {
        private final ReadWriteLock rwLock;
        // Acts as a cache, and used while listing members of the actual bucket.
        private final List<String> virtBuckets;

        public BucketInfo( ReadWriteLock rwLock, List<String> virtBuckets ) 
        {
            this.rwLock = rwLock;
            this.virtBuckets = virtBuckets;
        }
    }
    
    /**
     * Creates a consistent hashing ring.
    */
    public ConsistentHasherImpl()
    {
        this.bucketsMap = new ConcurrentSkipListMap<>();
        this.membersMap = new ConcurrentSkipListMap<>();
        this.bucketsAndLocks = new ConcurrentHashMap<>();
    }
    
    /**
     * Creates a consistent hashing ring with the specified initial capacity.
     */
    public ConsistentHasherImpl( int initialCapacity )
    {
        this.bucketsMap = new ConcurrentSkipListMap<>();
        this.membersMap = new ConcurrentSkipListMap<>();
        this.bucketsAndLocks = new ConcurrentHashMap<>( initialCapacity );
    }
    
    @Override
    public void addBucket( B bucketName, int virtualNodes )
    {
        Preconditions.checkNotNull( bucketName,    "bucket name cannot be null" );
        Preconditions.checkNotNull( virtualNodes, "virtualNodes cannot be null" );
        
        List<String> virtBuckets = new ArrayList<>();
        for (int virtualNodeId = 1; virtualNodeId <= virtualNodes; virtualNodeId++) {
            String virtBucket = DFSUtils.getNodeId( virtualNodeId, bucketName.getAddress() );
            bucketsMap.put( virtBucket, bucketName );
            virtBuckets.add( virtBucket );
        }
        
        bucketsAndLocks.put( bucketName, new BucketInfo( new ReentrantReadWriteLock(), virtBuckets ) );
    }
    
    @Override
    public B getBucket( String id )
    {
        return bucketsMap.get( id );
    }
    
    @Override
    public boolean containsBucket( B bucket )
    {
        return bucketsMap.containsKey( bucket.getId() );
    }

    @Override
    public void removeBucket( B bucketName ) throws InterruptedException 
    {
        removeBucket( bucketName, 0, null, false );
    }

    @Override
    public boolean tryRemoveBucket( B bucketName, long timeout, TimeUnit unit ) throws InterruptedException 
    {
        return removeBucket( bucketName, timeout, unit, true );
    }

    private boolean removeBucket( B bucketName, long timeout, TimeUnit unit, boolean tryLock ) throws InterruptedException 
    {
        Preconditions.checkNotNull( bucketName, "Bucket name cannot be null" );
        
        BucketInfo bucketInfo = bucketsAndLocks.remove( bucketName );
        if(bucketInfo == null)
            return true;
        
        ReadWriteLock rwLock = bucketInfo.rwLock;
        boolean result = false;
        try {
            if(tryLock)
                result = rwLock.writeLock().tryLock( timeout, unit );
            else {
                rwLock.writeLock().lock();
                result = true;
            }
            
            if(result) {
                for(String virtNode : bucketInfo.virtBuckets)
                    bucketsMap.remove( virtNode );
            }
        }
        finally {
            if(result)
                rwLock.writeLock().unlock();
        }
        
        return result;
    }

    @Override
    public void addMember( M memberName )
    {
        Preconditions.checkNotNull( memberName, "Member name cannot be null" );
        membersMap.put( DFSUtils.getId( memberName ), memberName );
    }

    @Override
    public void removeMember( M memberName )
    {
        Preconditions.checkNotNull( memberName, "Member name cannot be null" );
        membersMap.remove( DFSUtils.getId( memberName ) );
    }
    
    @Override
    public boolean containsMember( M member )
    {
        return membersMap.containsKey( DFSUtils.getId( member ) );
    }

    @Override
    public List<M> getMembersFor( B bucketName, List<? extends M> members )
    {
        Preconditions.checkNotNull( bucketName, "Bucket name cannot be null." );
        Preconditions.checkNotNull( members,        "Members cannot be null." );
        
        NavigableMap<String, M> localMembersMap = new TreeMap<>();
        members.forEach( member -> {
            localMembersMap.put( DFSUtils.getId( member ), member );
        });
        
        return getMembersInternal( bucketName, localMembersMap );
    }

    @Override
    public List<M> getMembersFor( B bucketName )
    {
        return getMembersInternal( bucketName, membersMap );
    }

    private List<M> getMembersInternal( B bucketName, NavigableMap<String, M> members )
    {
        Preconditions.checkNotNull( bucketName, "Bucket name cannot be null." );
        Preconditions.checkNotNull( members,        "Members cannot be null." );
        
        BucketInfo bInfo = bucketsAndLocks.get( bucketName );
        if (bInfo == null)
            return Collections.emptyList();

        ReadWriteLock rwLock = bInfo.rwLock;
        List<M> result = new ArrayList<>();
        try {
            rwLock.readLock().lock();
            if (bucketsAndLocks.containsKey( bucketName )) {
                for (String currNode : bInfo.virtBuckets) {
                    // get the previous key
                    String prevNode = bucketsMap.lowerKey( currNode );
                    if (prevNode == null) {
                        // add all the lower keys
                        result.addAll( members.headMap( currNode, true ).values() );
                        Optional<String> lastKey = getLastKey( bucketsMap );
                        if (lastKey.isPresent() && !lastKey.get().equals( currNode ))
                            result.addAll( members.tailMap( lastKey.get(), false ).values() ); // add all the greater keys
                    } else {
                        // add all the keys in the range prevNode - currNode
                        result.addAll(members.subMap( prevNode, false, currNode, true ).values());
                    }
                }
            }
        } finally {
            rwLock.readLock().unlock();
        }
        
        return result;
    }

    @Override
    public Map<B, List<M>> getAllBucketsToMembersMapping()
    {
        Map<B, List<M>> result = new HashMap<>();
        for (B bucket : bucketsAndLocks.keySet()) {
            List<M> members = getMembersFor( bucket );
            result.put( bucket, members );
        }
        
        return result;
    }

    @Override
    public List<B> getAllBuckets()
    {
        return new ArrayList<B>( bucketsMap.values() );
    }

    @Override
    public List<M> getAllMembers()
    {
        return new ArrayList<>( membersMap.values() );
    }
    
    @Override
    public String getFirstKey()
    {
        try{ return bucketsMap.firstKey(); }
        catch( NoSuchElementException e ) { return null; }
    }
    
    @Override
    public String getLastKey()
    {
        try{ return bucketsMap.lastKey(); }
        catch( NoSuchElementException e ) { return null; }
    }
    
    @Override
    public ArrayList<String> getSuccessors( String id )
    {
        Preconditions.checkNotNull( id, "Id can not be null" );
        return new ArrayList<String>( bucketsMap.tailMap( id ).keySet() );
    }
    
    @Override
    public String getSuccessor( String id )
    {
        Preconditions.checkNotNull( id, "Id cannot be null" );
        return bucketsMap.higherKey( id );
    }
    
    @Override
    public String getNextBucket( String id )
    {
        String succ = getSuccessor( id );
        if(succ == null)
            return getFirstKey();
        
        return succ;
    }
    
    @Override
    public ArrayList<String> getPredecessors( String id )
    {
        Preconditions.checkNotNull( id, "Id can not be null" );
        return new ArrayList<String>( bucketsMap.headMap( id ).keySet() );
    }
    
    @Override
    public String getPredecessor( String id )
    {
        Preconditions.checkNotNull( id, "Id can not be null" );
        return bucketsMap.lowerKey( id );
    }
    
    @Override
    public String getPreviousBucket( String id )
    {
        String prev = getPredecessor( id );
        if(prev == null)
            return getLastKey();
        
        return prev;
    }
    
    @Override
    public List<String> getVirtualBucketsFor( B bucketName )
    {
        Preconditions.checkNotNull( bucketName, "Bucket name can not be null" );
        return bucketsAndLocks.get( bucketName ).virtBuckets;
    }
    
    @Override
    public boolean isEmpty() {
        return bucketsMap.isEmpty();
    }
    
    @Override
    public int getBucketSize() {
        return bucketsMap.size();
    }
    
    @Override
    public int getMemberSize() {
        return membersMap.size();
    }

    @Override
    public void clear()
    {
        bucketsMap.clear();
        membersMap.clear();
        bucketsAndLocks.clear();
    }

    /**
     * Calculates the distribution of members to buckets for various virtual
     * nodes, and returns distribution buckets and corresponding members list
     * for each virtual node in a map.
     * 
     * @param startVirtNodeId
     * @param endVirtNodeId
     * @param bucketDataToBytesConverter
     * @param memberDataToBytesConverter
     * @param hashFunction
     * @param buckets
     * @param members
     * @return map of virtual node ids and corresponding distribution map. Value
     *         contains a map of bucket names and corresponding members.
     */
    public static <B  extends GossipMember, M extends Serializable> Map<Integer, Map<B, List<M>>> getDistribution(
            int startVirtNodeId, 
            int endVirtNodeId,
            BytesConverter<B> bucketDataToBytesConverter,
            BytesConverter<M> memberDataToBytesConverter,
            HashFunction hashFunction, List<? extends B> buckets,
            List<? extends M> members ) 
    {
        Map<Integer, Map<B, List<M>>> result = new HashMap<>();
        for (int virtNodeId = startVirtNodeId; virtNodeId <= endVirtNodeId; virtNodeId++) {
            ConsistentHasher<B, M> cHasher = new ConsistentHasherImpl<>();
            buckets.stream().forEach( bucketName -> cHasher.addBucket( bucketName, endVirtNodeId - startVirtNodeId ) );
            members.stream().forEach( memberName -> cHasher.addMember( memberName ) );
            Map<B, List<M>> distribution = cHasher.getAllBucketsToMembersMapping();
            result.put( virtNodeId, distribution );
        }
        return result;
    }

    /**
     * Calculates the distribution of members to buckets for various virtual
     * nodes, and returns distribution of buckets and corresponding count for
     * each virtual node in a map.
     * 
     * @param startVirtNodeId
     * @param endVirtNodeId
     * @param bucketDataToBytesConverter
     * @param memberDataToBytesConverter
     * @param hashFunction
     * @param buckets
     * @param members
     * @return map of virtual node ids and corresponding distribution map. Value
     *         contains a map of bucket names and corresponding members size.
     */
    public static <B extends GossipMember, M extends Serializable> Map<Integer, Map<Integer, B>> getDistributionCount(
            int startVirtNodeId, 
            int endVirtNodeId,
            BytesConverter<B> bucketDataToBytesConverter,
            BytesConverter<M> memberDataToBytesConverter,
            HashFunction hashFunction, 
            List<? extends B> buckets,
            List<? extends M> members ) 
    {
        Map<Integer, Map<B, List<M>>> distribution = getDistribution(
                startVirtNodeId, 
                endVirtNodeId, 
                bucketDataToBytesConverter,
                memberDataToBytesConverter, 
                hashFunction, 
                buckets, 
                members );
        Map<Integer, Map<Integer, B>> result = new TreeMap<>();
        distribution.forEach(( vnSize, map ) -> {
            Map<Integer, B> pResult = new TreeMap<>();
            map.forEach(( b, list ) -> {
                pResult.put( list.size(), b );
            });
            result.put( vnSize, pResult );
        });
        return result;
    }

    /**
     * Calculates the distribution of members to buckets for various virtual
     * nodes, and returns distribution of buckets and corresponding percentage
     * for each virtual node in a map.
     * 
     * @param startVirtNodeId
     * @param endVirtNodeId
     * @param bucketDataToBytesConverter
     * @param memberDataToBytesConverter
     * @param hashFunction
     * @param buckets
     * @param members
     * @return map of virtual node ids and corresponding distribution map. Value
     *         contains a map of bucket names and corresponding percentage of
     *         members.
     */
    public static <B extends GossipMember, M extends Serializable> Map<Integer, Map<Double, B>> getDistributionPercentage(
            int startVirtNodeId, 
            int endVirtNodeId,
            BytesConverter<B> bucketDataToBytesConverter,
            BytesConverter<M> memberDataToBytesConverter,
            HashFunction hashFunction, 
            List<? extends B> buckets,
            List<? extends M> members ) 
    {
        Map<Integer, Map<B, List<M>>> distribution = getDistribution(
                startVirtNodeId, 
                endVirtNodeId, 
                bucketDataToBytesConverter,
                memberDataToBytesConverter, 
                hashFunction, 
                buckets, 
                members );
        Map<Integer, Map<Double, B>> result = new TreeMap<>();
        distribution.forEach((vnSize, map) -> {
            Map<Double, B> pResult = new TreeMap<>();
            map.forEach(( b, list ) -> {
                double percentage = ((double) list.size() / (double) members.size()) * 100;
                pResult.put( percentage, b );
            });
            result.put( vnSize, pResult );
        });
        return result;
    }

    /**
     * {@link NavigableMap#lastKey()} throws {@link NoSuchElementException} in
     * case if the map is empty. This function just wraps up the lastKey if the
     * value is present, or null inside the Optional and returns the result.
     * 
     * @param map
     * @return
     */
    private static <T> Optional<T> getLastKey( NavigableMap<T, ?> map ) 
    {
        T key = null;
        try {
            if (!map.isEmpty())
                key = map.lastKey();
        } catch (NoSuchElementException e) {
            // Intentionally ignored.
        }
        Optional<T> result = Optional.ofNullable( key );
        return result;
    }
}
