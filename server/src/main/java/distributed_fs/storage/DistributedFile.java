/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import distributed_fs.net.IOSerializable;
import distributed_fs.overlay.manager.anti_entropy.MerkleTree;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;
import distributed_fs.versioning.Version;

/**
 * Class used to store a file on database and to send a file to a remote node,
 * containing the serialization of the file and its vector clock.
*/
public class DistributedFile implements Serializable, IOSerializable
{
	private String name;
	private VectorClock version;
	private boolean deleted = false;
	private boolean isDirectory;
	
	// Content of the file on disk.
	private transient byte[] content = null;
	
	private String fileId;
	private String HintedHandoff;
	
	private transient byte[] signature;
	
	// Parameters used to check the life of a deleted file.
	private long currTime, liveness;
	private static transient final int TTL = 3600000; // 1 hour
	
	// Generated serial ID.
    private static final long serialVersionUID = 7522473187709784849L;
    
    
    
	
    /**
     * Constructor.
    */
    public DistributedFile( final byte[] data )
    {
        write( data );
    }
	
	/**
     * Constructor.
    */
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
	
	public void loadContent( final DBManager db ) throws IOException
    {
	    if(!deleted && !isDirectory) {
            byte[] file = db.readFileFromDisk( name );
            // Store the content in compressed form.
            content = DFSUtils.compressData( file );
	    }
    }
	
	/**
     * Returns the content of the file as a byte array.<br>
     * If the file has a content,
     * then it will be returned in decompressed form.
    */
    public byte[] getContent() throws IOException
    {
        if(content == null)
            return null;
        else
            return DFSUtils.decompressData( content );
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
	
	public boolean isDirectory() {
    	return isDirectory;
    }

    public boolean isDeleted() {
		return deleted;
	}
	
	public void setDeleted( final boolean value )
	{
		if(deleted != value) {
			deleted = value;
			if(deleted) {
				content = null;
			    currTime = System.currentTimeMillis();
			}
			else
				liveness = 0;
		}
	}
	
	/**
	 * Method used to check whether a file has to be definitely removed.
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
	 * Updated its current timestamp.<br>
	 * It's useful when the file is transferred in another server.
	*/
	public void setCurrentTime()
	{
	    currTime = System.currentTimeMillis();
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
	public void incrementVersion( final String nodeId )
	{
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
    public byte[] read()
    {
        int contentSize = (content == null) ? 0 : (content.length + Integer.BYTES);
        int hhSize = (HintedHandoff == null) ? 0 : (HintedHandoff.length() + Integer.BYTES);
        
        byte[] clock = DFSUtils.serializeObject( version );
        ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES * 3 + Long.BYTES * 2 + Byte.BYTES * 3 +
                                                 name.length() + clock.length + fileId.length() + hhSize + contentSize );
        
        buffer.putInt( name.length() ).put( name.getBytes( StandardCharsets.UTF_8 ) );
        buffer.putInt( clock.length ).put( clock );
        buffer.put( (deleted) ? (byte) 0x1 : (byte) 0x0 );
        buffer.put( (isDirectory) ? (byte) 0x1 : (byte) 0x0 );
        buffer.putInt( fileId.length() ).put( fileId.getBytes( StandardCharsets.UTF_8 ) );
        buffer.putLong( currTime );
        buffer.putLong( liveness );
        
        buffer.put( (byte) ((hhSize == 0) ? 0x0 : 0x1) );
        if(hhSize > 0)
            buffer.putInt( HintedHandoff.length() ).put( HintedHandoff.getBytes( StandardCharsets.UTF_8 ) );
        
        if(contentSize > 0)
            buffer.putInt( content.length ).put( content );
        
        return buffer.array();
    }

    @Override
    public void write( byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        name = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
        version = DFSUtils.deserializeObject( DFSUtils.getNextBytes( buffer ) );
        deleted = (buffer.get() == (byte) 0x1);
        isDirectory = (buffer.get() == (byte) 0x1);
        fileId = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
        currTime = buffer.getLong();
        liveness = buffer.getLong();
        
        if(buffer.get() == (byte) 0x1)
            HintedHandoff = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
        
        if(buffer.remaining() > 0)
            content = DFSUtils.getNextBytes( buffer );
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
}