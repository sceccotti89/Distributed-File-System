/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import distributed_fs.anti_entropy.MerkleTree;
import distributed_fs.net.IOSerializable;
import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Version;

public class DistributedFile implements IOSerializable//, Serializable
{
	private String name;
	private VectorClock version;
	private boolean deleted = false;
	private boolean isDirectory;
	//private transient byte[] signature;
	//private final byte[] id;
	private transient String HintedHandoff;
	
	// Parameters used to check the life of a deleted file
	private transient long currTime, liveness = 0;
	private static transient final int TTL = 360000; // 1 hour
	
	//private static final long serialVersionUID = -1525749756439181410L;
	
	public DistributedFile( final byte[] data )
	{
		write( data );
	}
	
	public DistributedFile( final RemoteFile file, final String root )
	{
		this( file.getName(), root, file.getVersion() );
	}
	
	/**
	 * Constructor.
	 * This is equivalent to: {@link DistributedFile#DistributedFile(String, String, VectorClock, boolean)},
	 * with {@code false} as third parameter.
	*/
	public DistributedFile( final String name, final String root, final VectorClock version )
	{
		/*this( name, root, version, false );
	}

	public DistributedFile( final String name, final String root, final VectorClock version, final boolean set_signature )
	{*/
		this.name = name;
		this.version = version;
		
		isDirectory = new File( root + name ).isDirectory();
		if(isDirectory && !name.endsWith( "/" ))
			this.name += "/";
		
		//id = Utils.getId( this.name ).array();
		
		//if(set_signature)
			//signature = MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
			//signature = MerkleTree.getSignature( (name + version.toString()).getBytes( StandardCharsets.UTF_8 ) );
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
	public int getTimeToLive()
	{
		return (int) Math.max( 0, TTL - liveness );
	}
	
	public byte[] getSignature()
	{
		/*if(signature == null)
			signature = MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
		return signature;*/
		//return MerkleTree.getSignature( (name + version.toString() + deleted).getBytes( StandardCharsets.UTF_8 ) );
		return MerkleTree.getSignature( name.getBytes( StandardCharsets.UTF_8 ) );
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

	@Override
	public byte[] read()
	{
		byte[] clock = Utils.serializeObject( version );
		int hintedHandoffSize = (HintedHandoff == null) ? 0 : (HintedHandoff.length() + Integer.BYTES);
		ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES * 2 + name.length() + clock.length + Byte.BYTES * 2 + hintedHandoffSize );
		buffer.putInt( name.length() ).put( name.getBytes( StandardCharsets.UTF_8 ) );
		buffer.putInt( clock.length ).put( clock );
		buffer.put( (deleted) ? (byte) 0x1 : (byte) 0x0 );
		buffer.put( (isDirectory) ? (byte) 0x1 : (byte) 0x0 );
		if(hintedHandoffSize > 0)
			buffer.putInt( HintedHandoff.length() ).put( HintedHandoff.getBytes( StandardCharsets.UTF_8 ) );
		
		return buffer.array();
	}

	@Override
	public void write( byte[] data )
	{
		ByteBuffer buffer = ByteBuffer.wrap( data );
		name = new String( Utils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
		version = Utils.deserializeObject( Utils.getNextBytes( buffer ) );
		deleted = (buffer.get() == (byte) 0x1);
		isDirectory = (buffer.get() == (byte) 0x1);
		if(buffer.remaining() > 0)
			HintedHandoff = new String( Utils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
	}
}