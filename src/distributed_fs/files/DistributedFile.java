/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import distributed_fs.merkle_tree.MerkleTree;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Version;

public class DistributedFile implements Serializable
{
	private String name;
	private VectorClock version;
	private boolean deleted = false;
	private final boolean isDirectory;
	private transient byte[] signature;
	private transient String HintedHandoff;
	private transient byte[] id;
	
	// Parameters used to check the life of a deleted file
	private transient long currTime, liveness = 0;
	private static transient final int TTL = 360000; // 1 hour
	
	private static final long serialVersionUID = -1525749756439181410L;
	
	public DistributedFile( final RemoteFile file )
	{
		this( file.getName(), file.getVersion(), false );
	}
	
	/**
	 * Constructor.
	 * This is equivalent to: {@link DistributedFile#DistributedFile(String, VectorClock, boolean)},
	 * with {@code false} as third parameter.
	*/
	public DistributedFile( final String name, final VectorClock version )
	{
		this( name, version, false );
	}

	public DistributedFile( final String name, final VectorClock version, final boolean set_signature )
	{
		this.name = name;
		this.version = version;
		
		this.isDirectory = new File( name ).isDirectory();
		if(isDirectory && !name.endsWith( "/" ))
			this.name += "/";
		
		this.id = Utils.getId( this.name ).array();
		
		if(set_signature)
			signature = MerkleTree.getSignature( name.getBytes() );
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
	
	public byte[] getId() {
		return id;
	}
	
	public boolean isDeleted() {
		return deleted;
	}
	
	public void setDeleted( final boolean value )
	{
		deleted = value;
		if(deleted)
			currTime = System.currentTimeMillis();
		else
			liveness = 0;
	}
	
	public boolean isDirectory() {
		return isDirectory;
	}
	
	/**
	 * Method used to check whether a file has to be delete.
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
	public int getTimeToLive() {
		return (int) Math.max( 0, TTL - liveness );
	}
	
	public byte[] getSignature()
	{
		if(signature == null)
			signature = MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
		return signature;
	}
	
	public VectorClock getVersion() {
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
		return "{Name: " + name +
				", Version: " + version.toString() +
				", Directory: " + isDirectory +
				", Deleted: " + deleted + "}";
	}
}