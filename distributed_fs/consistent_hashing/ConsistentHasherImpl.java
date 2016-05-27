
package distributed_fs.consistent_hashing;

import java.io.Serializable;
import java.nio.ByteBuffer;
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

import distributed_fs.utils.Utils;
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
 * @param <GossipMember>	bucket type.
 * @param <M>				member type.
 */
// @ThreadSafe

public class ConsistentHasherImpl<B extends GossipMember, M extends Serializable> implements ConsistentHasher<B, M> 
{
	private final NavigableMap<ByteBuffer, B> bucketsMap;
	private final NavigableMap<ByteBuffer, M> membersMap;
	
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
		private final List<ByteBuffer> virtBuckets;

		public BucketInfo( final ReadWriteLock rwLock, final List<ByteBuffer> virtBuckets ) 
		{
			this.rwLock = rwLock;
			this.virtBuckets = virtBuckets;
		}
	}

	/**
	 * Creates a consistent hashing ring with the specified hashfunction and
	 * bytesconverter objects.
	 * 
	 * @param virtualInstancesPerBucket creates specified number of virtual
	 * buckets for each bucket. More the virtual instances, more the equal 
	 * distribution among buckets. Should be greater than zero, otherwise 
	 * value 1 is used.
	 * 
	 * @param bucketDataToBytesConverter
	 * @param memberDataToBytesConverter
	 * @param hashFunction	is used to hash the given bucket and member.
	 */
	public ConsistentHasherImpl()
	{
		this.bucketsMap = new ConcurrentSkipListMap<>();
		this.membersMap = new ConcurrentSkipListMap<>();
		this.bucketsAndLocks = new ConcurrentHashMap<>();
	}
	
	@Override
	public void addBucket( final B bucketName, final int virtualNodes )
	{
		Preconditions.checkNotNull( bucketName, "Bucket name can not be null" );
		Preconditions.checkNotNull( virtualNodes, "Bucket name can not be null" );
		
		List<ByteBuffer> virtBuckets = new ArrayList<>();
		for (int virtualNodeId = 1; virtualNodeId <= virtualNodes; virtualNodeId++) {
			ByteBuffer virtBucket = Utils.getNodeId( virtualNodeId, bucketName.getAddress() );
			//System.out.println( "Value: " + Utils.bytesToHex( virtBucket.array() ) );
			bucketsMap.put( virtBucket, bucketName );
			virtBuckets.add( virtBucket );
		}
		
		bucketsAndLocks.putIfAbsent( bucketName, new BucketInfo( new ReentrantReadWriteLock(), virtBuckets ) );
	}
	
	@Override
	public B getBucket( final ByteBuffer id )
	{
		return bucketsMap.get( id );
	}

	@Override
	public void removeBucket( final B bucketName ) throws InterruptedException 
	{
		removeBucket( bucketName, 0, null, false );
	}

	@Override
	public boolean tryRemoveBucket( final B bucketName, final long timeout, final TimeUnit unit ) throws InterruptedException 
	{
		return removeBucket( bucketName, timeout, unit, true );
	}

	private boolean removeBucket( final B bucketName, final long timeout, final TimeUnit unit, final boolean tryLock ) throws InterruptedException 
	{
		Preconditions.checkNotNull( bucketName, "Bucket name can not be null" );
		
		BucketInfo bucketInfo = bucketsAndLocks.remove( bucketName );
		if(bucketInfo == null)
			return true;
		
		ReadWriteLock rwLock = bucketInfo.rwLock;
		boolean result = false;
		try {
			if (tryLock)
				result = rwLock.writeLock().tryLock( timeout, unit );
			else {
				rwLock.writeLock().lock();
				result = true;
			}
			if (result)
				for (ByteBuffer virtNode : bucketInfo.virtBuckets)
					bucketsMap.remove( virtNode );
		} finally {
			if (result)
				rwLock.writeLock().unlock();
		}
		
		return result;
	}

	@Override
	public void addMember( final M memberName )
	{
		Preconditions.checkNotNull( memberName, "Member name can not be null" );
		membersMap.put( Utils.getId( memberName ), memberName );
	}

	@Override
	public void removeMember( final M memberName )
	{
		Preconditions.checkNotNull( memberName, "Member name can not be null" );
		membersMap.remove( Utils.getId( memberName ) );
	}

	@Override
	public List<M> getMembersFor( final B bucketName, final List<? extends M> members )
	{
		Preconditions.checkNotNull( bucketName, "Bucket name can not be null." );
		Preconditions.checkNotNull( members,	    "Members can not be null." );
		
		NavigableMap<ByteBuffer, M> localMembersMap = new TreeMap<>();
		members.forEach( member -> {
			localMembersMap.put( Utils.getId( member ), member );
		});
		
		return getMembersInternal( bucketName, localMembersMap );
	}

	@Override
	public List<M> getMembersFor( final B bucketName ) 
	{
		return getMembersInternal( bucketName, membersMap );
	}

	private List<M> getMembersInternal( final B bucketName, final NavigableMap<ByteBuffer, M> members ) 
	{
		Preconditions.checkNotNull( bucketName, "Bucket name can not be null." );
		Preconditions.checkNotNull( members,	    "Members can not be null." );
		
		BucketInfo bInfo = bucketsAndLocks.get( bucketName );
		if (bInfo == null)
			return Collections.emptyList();

		ReadWriteLock rwLock = bInfo.rwLock;
		List<M> result = new ArrayList<>();
		try {
			rwLock.readLock().lock();
			if (bucketsAndLocks.containsKey( bucketName )) {
				for (ByteBuffer currNode : bInfo.virtBuckets) {
					// get the previous key
					ByteBuffer prevNode = bucketsMap.lowerKey( currNode );
					if (prevNode == null) {
						// add all the lower keys
						result.addAll( members.headMap( currNode, true ).values() );
						Optional<ByteBuffer> lastKey = getLastKey( bucketsMap );
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
		return new ArrayList<B>( bucketsAndLocks.keySet() );
	}

	@Override
	public List<M> getAllMembers() 
	{
		return new ArrayList<>( membersMap.values() );
	}
	
	public ByteBuffer getFirstKey()
	{
		try{ return bucketsMap.firstKey(); }
		catch( NoSuchElementException e ) { return null; }
	}
	
	public ByteBuffer getLastKey()
	{
		try{ return bucketsMap.lastKey(); }
		catch( NoSuchElementException e ) { return null; }
	}
	
	/**
	 * Returns the successor buckets of the given bucket id.
	 * 
	 * @param id	the given bucket
	*/
	public ArrayList<ByteBuffer> getSuccessors( final ByteBuffer id )
	{
		Preconditions.checkNotNull( id, "Id can not be null" );
		return new ArrayList<ByteBuffer>( bucketsMap.tailMap( id ).keySet() );
	}
	
	/**
	 * Returns the successor bucket of the given id.
	 * 
	 * @param id	the given identifier
	*/
	public ByteBuffer getSuccessor( final ByteBuffer id )
	{
		Preconditions.checkNotNull( id, "Id cannot be null" );
		return bucketsMap.higherKey( id );
	}
	
	/**
	 * Returns the successor bucket of the given bucket id.<br>
	 * If there is no other nodes, checks it from the first key.
	 * 
	 * @param id	the given bucket
	*/
	public ByteBuffer getNextBucket( final ByteBuffer id )
	{
		ByteBuffer succ = getSuccessor( id );
		if(succ == null)
			return getFirstKey();
		
		return succ;
	}
	
	/**
	 * Returns the predecessor buckets of the given bucket id.
	 * 
	 * @param id	the given bucket
	*/
	public ArrayList<ByteBuffer> getPredecessors( final ByteBuffer id )
	{
		Preconditions.checkNotNull( id, "Id can not be null" );
		return new ArrayList<ByteBuffer>( bucketsMap.headMap( id ).keySet() );
	}
	
	/**
	 * Returns the predecessor bucket of the given bucket id.
	 * 
	 * @param id	the given bucket
	*/
	public ByteBuffer getPredecessor( final ByteBuffer id )
	{
		Preconditions.checkNotNull( id, "Id can not be null" );
		return bucketsMap.lowerKey( id );
	}
	
	/**
	 * Returns the predecessor bucket of the given bucket id.<br>
	 * If there is no other nodes, checks it from the last key.
	 * 
	 * @param id	the given bucket
	*/
	public ByteBuffer getPreviousBucket( final ByteBuffer id )
	{
		ByteBuffer prev = getPredecessor( id );
		if(prev == null)
			return getLastKey();
		
		return prev;
	}
	
	@Override
	public List<ByteBuffer> getVirtualBucketsFor( final B bucketName )
	{
		Preconditions.checkNotNull( bucketName, "Bucket name can not be null" );
		return bucketsAndLocks.get( bucketName ).virtBuckets;
	}
	
	/**
	 * Returns the size of the database.
	*/
	public int getSize() {
		return bucketsMap.size();
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
			final int startVirtNodeId, 
			final int endVirtNodeId,
			final BytesConverter<B> bucketDataToBytesConverter,
			final BytesConverter<M> memberDataToBytesConverter,
			final HashFunction hashFunction, List<? extends B> buckets,
			final List<? extends M> members ) 
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
			final int startVirtNodeId, 
			final int endVirtNodeId,
			final BytesConverter<B> bucketDataToBytesConverter,
			final BytesConverter<M> memberDataToBytesConverter,
			final HashFunction hashFunction, 
			final List<? extends B> buckets,
			final List<? extends M> members ) 
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
			final int startVirtNodeId, 
			final int endVirtNodeId,
			final BytesConverter<B> bucketDataToBytesConverter,
			final BytesConverter<M> memberDataToBytesConverter,
			final HashFunction hashFunction, 
			final List<? extends B> buckets,
			final List<? extends M> members ) 
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
	private static <T> Optional<T> getLastKey( final NavigableMap<T, ?> map ) 
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