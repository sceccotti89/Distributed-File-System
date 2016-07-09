/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import distributed_fs.overlay.manager.anti_entropy.MerkleTree;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Version;

public class DistributedFile implements /*IOSerializable,*/ Serializable
{
	private String name;
	private VectorClock version;
	private boolean deleted = false;
	private boolean isDirectory;
	
	private String fileId;
	private String HintedHandoff;
	
	private transient byte[] signature;
	
	// Parameters used to check the life of a deleted file.
	private long currTime, liveness;
	private static transient final int TTL = 3600000; // 1 hour
	
	/* Generated Serial ID */
    private static final long serialVersionUID = 7522473187709784849L;

    /**
	 * Constructor used when the file has been serialized with
	 * the {@link IOSerializable} interface.
	*/
	/*public DistributedFile( final byte[] data )
	{
		write( data );
		fileId = DFSUtils.byteBufferToHex( DFSUtils.getId( this.name ) );
	}*/
	
    /**
     * General constructor.
    */
	public DistributedFile( final RemoteFile file, final boolean isDirectory, final String hintedHandoff )
	{
		this( file.getName(), isDirectory, file.getVersion(), hintedHandoff );
	}
	
	public DistributedFile( final String name, final boolean isDirectory, final VectorClock version, final String hintedHandoff )
	{
		this.name = name;
		this.version = version;
		this.HintedHandoff = hintedHandoff;
		
		this.isDirectory = isDirectory;
		if(isDirectory && !name.endsWith( "/" ))
			this.name += "/";
		
		fileId = DFSUtils.getId( this.name );
	}
	
	public void setHintedHandoff( final String address ) {
		HintedHandoff = address;
	}
	
	public String getHintedHandoff() {
		return HintedHandoff;
	}
	
	public String getName() {
		return name;
	}
	
	public String getId() {
	    return fileId;
	}
	
	public boolean isDeleted() {
		return deleted;
	}
	
	public void setDeleted( final boolean value )
	{
		if(deleted != value) {
			deleted = value;
			if(deleted)
				currTime = System.currentTimeMillis();
			else
				liveness = 0;
		}
	}
	
	public boolean isDirectory() {
		return isDirectory;
	}
	
	/**
	 * Method used to check whether a file has to be definitely deleted.
	 * If the file is marked as deleted, then it can be removed
	 * when an internal TTL value will be reached.
	 * 
	 * @return {@code true} if the file has to be removed,
	 *		   {@code false} otherwise
	*/
	public boolean checkDelete()
	{
		if(isDeleted()) {
			long timestamp = System.currentTimeMillis();
			liveness += (timestamp - currTime);
			currTime = timestamp;
		}
		
		return liveness >= TTL;
	}
	
	/**
	 * Returns the Time To Live of the file.<br>
	 * It's meaningful only if the file has been marked as deleted.
	*/
	public int getTimeToLive()
	{
		return (int) Math.max( 0, TTL - liveness );
	}
	
	/**
	 * Returns the signature of the file.
	*/
	public byte[] getSignature()
	{
		if(signature == null)
			signature = MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
		return signature;
		//return MerkleTree.getSignature( (name + version.toString() + deleted).getBytes( StandardCharsets.UTF_8 ) );
		//return MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
	}
	
	public VectorClock getVersion()
	{
		return version;
	}
	
	/** 
	 * Sets a new version of the file.
	 * 
	 * @param version	the new version
	*/
	public void setVersion( final Version version )
	{
		if(version instanceof VectorClock)
			this.version = (VectorClock) version;
	}
	
	/** 
	 * Increments the current version of the vector clock.
	 * 
	 * @param nodeId	the version to increment
	*/
	public void incrementVersion( final String nodeId ) {
		version.incrementVersion( nodeId );
	}
	
	@Override
	public boolean equals( final Object o )
	{
		if(!(o instanceof DistributedFile))
			return false;
		
		return o.toString().equals( toString() );
	}
	
	@Override
	public String toString()
	{
		return "{ Name: " + name +
				", Version: " + version.toString() +
				", Directory: " + isDirectory +
				", HintedHandoff: " + HintedHandoff +
				", Deleted: " + deleted + " }";
	}

	/*@Override
	public byte[] read()
	{
		byte[] clock = DFSUtils.serializeObject( version );
		int hintedHandoffSize = (HintedHandoff == null) ? 0 : (HintedHandoff.length() + Integer.BYTES);
		int capacity = Integer.BYTES * 2 + name.length() + clock.length + Byte.BYTES * 2 + Long.BYTES * 2 + hintedHandoffSize;
		ByteBuffer buffer = ByteBuffer.allocate( capacity );
		buffer.putInt( name.length() ).put( name.getBytes( StandardCharsets.UTF_8 ) );
		buffer.putInt( clock.length ).put( clock );
		buffer.put( (deleted) ? (byte) 0x1 : (byte) 0x0 );
		if(deleted) {
    		buffer.putLong( currTime );
    		buffer.putLong( liveness );
		}
		buffer.put( (isDirectory) ? (byte) 0x1 : (byte) 0x0 );
		if(hintedHandoffSize > 0)
			buffer.putInt( HintedHandoff.length() ).put( HintedHandoff.getBytes( StandardCharsets.UTF_8 ) );
		
		return buffer.array();
	}

	@Override
	public void write( byte[] data )
	{
		ByteBuffer buffer = ByteBuffer.wrap( data );
		name = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
		version = DFSUtils.deserializeObject( DFSUtils.getNextBytes( buffer ) );
		deleted = (buffer.get() == (byte) 0x1);
		if(deleted) {
            currTime = buffer.getLong();
            liveness = buffer.getLong();
        }
		isDirectory = (buffer.get() == (byte) 0x1);
		if(buffer.remaining() > 0)
			HintedHandoff = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
	}*/
}