/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import distributed_fs.net.IOSerializable;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;

/**
 * Class used to send a remote file,
 * containing the serialization of the file and its vector clock
*/
public class RemoteFile implements IOSerializable//, Serializable
{
	private String name;
	private byte[] content;
	private VectorClock vClock;
	private boolean deleted;
	private boolean isDirectory;
	
	//private static final long serialVersionUID = 2640027211348418180L;
	
	public RemoteFile( final byte[] data )
	{
		write( data );
	}

	public RemoteFile( final DistributedFile file, final String dbRoot ) throws IOException
	{
		this( file.getName(), file.getVersion(), file.isDeleted(), file.isDirectory(), dbRoot );
	}

	public RemoteFile( final String name, final VectorClock vClock,
					   final boolean removed, final boolean directory, final String dbRoot ) throws IOException
	{
		this.name = name;
		this.vClock = vClock;
		
		this.isDirectory = directory;
		if(isDirectory && !name.endsWith( "/" ))
			this.name += "/";
		
		this.deleted = removed;
		
		if(removed || directory)
			this.content = null;
		else {
			byte[] file = DFSUtils.readFileFromDisk( dbRoot + name );
			// Store the content in compressed form.
			this.content = DFSUtils.compressData( file );
		}
	}
	
	public String getName()
	{
		return name;
	}
	
	public void setName( final String name )
	{
		this.name = name;
	}
	
	public boolean isDirectory()
	{
		return isDirectory;
	}
	
	public VectorClock getVersion()
	{
		return vClock;
	}
	
	public void setVersion( final VectorClock version )
	{
		vClock = version;
	}
	
	/**
	 * Increments the version info associated with the given node.
	 * 
	 * @param nodeId	node's identifier
	*/
	public void incrementVersion( final String nodeId )
	{
		vClock.incrementVersion( nodeId );
	}
	
	/**
	 * Checks whether the file has been deleted.
	*/
	public boolean isDeleted()
	{
		return deleted;
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
	
	@Override
	public String toString()
	{
		return "{ Name: " + name +
				", Version: " + vClock.toString() +
				", Directory: " + isDirectory + 
				". Deleted: " + isDeleted() + " }";
	}

	@Override
	public byte[] read()
	{
		int contentSize = (content == null) ? 0 : (content.length + Integer.BYTES);
		byte[] clock = DFSUtils.serializeObject( vClock );
		ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES * 2 + name.length() + clock.length + Byte.BYTES * 2 + contentSize );
		buffer.putInt( name.length() ).put( name.getBytes( StandardCharsets.UTF_8 ) );
		buffer.putInt( clock.length ).put( clock );
		buffer.put( (deleted) ? (byte) 0x1 : (byte) 0x0 );
		buffer.put( (isDirectory) ? (byte) 0x1 : (byte) 0x0 );
		if(contentSize > 0)
			buffer.putInt( content.length ).put( content );
		
		return buffer.array();
	}

	@Override
	public void write( byte[] data )
	{
		ByteBuffer buffer = ByteBuffer.wrap( data );
		name = new String( DFSUtils.getNextBytes( buffer ), StandardCharsets.UTF_8 );
		vClock = DFSUtils.deserializeObject( DFSUtils.getNextBytes( buffer ) );
		deleted = (buffer.get() == (byte) 0x1);
		isDirectory = (buffer.get() == (byte) 0x1);
		if(buffer.remaining() > 0)
			content = DFSUtils.getNextBytes( buffer );
	}
}