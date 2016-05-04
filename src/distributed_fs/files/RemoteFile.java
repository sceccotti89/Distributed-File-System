/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.io.IOException;
import java.io.Serializable;

import distributed_fs.utils.Utils;
import distributed_fs.versioning.VectorClock;

/**
 * Class used to send a remote file,
 * containing the serialization of the file and its vector clock
*/
public class RemoteFile implements Serializable
{
	private String name;
	private final byte[] content;
	private final VectorClock vClock;
	private final boolean isDirectory;
	
	private static final long serialVersionUID = 2640027211348418180L;

	public RemoteFile( final DistributedFile file ) throws IOException
	{
		this( file.getName(), file.getVersion(), file.isDeleted(), file.isDirectory() );
	}

	public RemoteFile( final String name, final VectorClock vClock,
					   final boolean removed, final boolean directory ) throws IOException
	{
		this.name = name;
		this.vClock = vClock;
		
		this.isDirectory = directory;
		if(isDirectory && !name.endsWith( "/" ))
			this.name += "/";
		
		if(removed || directory)
			this.content = null;
		else {
			byte[] file = Utils.readFileFromDisk( name );
			// store the content in compressed form
			this.content = Utils.compressData( file );
		}
	}
	
	/**
	 *  Returns the name of the file.
	*/
	public String getName()
	{
		return name;
	}
	
	public boolean isDirectory()
	{
		return isDirectory;
	}
	
	/** 
	 * Returns the associated version.
	*/
	public VectorClock getVersion()
	{
		return vClock;
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
		return content == null;
	}
	
	/** 
	 * Returns the content of the file as a byte array.<br>
	 * If the file has a content, then it will be returned in decompressed form.
	*/
	public byte[] getContent()
	{
		if(content == null)
			return null;
		else
			return Utils.decompressData( content );
	}
}