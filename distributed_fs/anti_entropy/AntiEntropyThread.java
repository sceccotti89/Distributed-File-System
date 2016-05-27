/**
 * @author Stefano Ceccotti
*/

package distributed_fs.anti_entropy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import distributed_fs.consistent_hashing.ConsistentHasherImpl;
import distributed_fs.net.Networking.TCPnet;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.storage.FileManagerThread;
import distributed_fs.utils.Utils;
import gossiping.GossipMember;

/**
 * Abstract class used to execute the anti-entropy machanism,
 * performed with the Merkle tree exchanging protocol.
*/
public abstract class AntiEntropyThread extends Thread
{
	protected final ConsistentHasherImpl<GossipMember, String> cHasher;
	protected final FileManagerThread fMgr;
	protected final DFSDatabase database;
	protected final TCPnet Net;
	protected GossipMember me;
	private final Random random;
	protected boolean shoutDown = false;
	
	/** Port used to exchange the Merkle tree */
	//protected static final int MERKLE_TREE_EXCHANGE_PORT = 8000;
	/** Type of messages exchanged during the synchronization procedure */
	protected static final byte MERKLE_FROM_MAIN = 0x0, MERKLE_FROM_REPLICA = 0x1;
	
	/** Logger used to print the application state. */
	protected static final Logger LOGGER = Logger.getLogger( AntiEntropyThread.class.getName() );
	
	public AntiEntropyThread( final GossipMember _me,
							  final DFSDatabase database,
							  final FileManagerThread fMgr,
							  final ConsistentHasherImpl<GossipMember, String> _cHasher )
	{
		if(!Utils.testing)
			LOGGER.setLevel( Utils.logLevel );
		
		this.me = _me;
		this.fMgr = fMgr;
		this.database = database;
		Net = new TCPnet();
		cHasher = _cHasher;
		random = new Random();
	}
	
	/** 
	 * Creates the Merkle tree corresponding to the input files.
	 * 
	 * @param files		files used to create the Merkle tree
	*/
	protected MerkleTree createMerkleTree( final List<DistributedFile> files )
	{
		if(files == null || files.size() == 0)
			return null;
		
		List<byte[]> bytes = new ArrayList<>( files.size() );
		for(DistributedFile file : files)
			bytes.add( file.getSignature() );
		
		return new MerkleTree( bytes );
	}
	
	/**
	 * [The selectToSend() function.] Find a random peer from the local
	 * membership list. In the case where this client is the only member in the
	 * list, this method will return null.
	 *
	 * @return Member random member if list is greater than 1, null otherwise
	 */
	protected ByteBuffer selectPartner( final List<ByteBuffer> memberList ) 
	{
		ByteBuffer member = null;
		if (memberList.size() > 0) {
			int randomNeighborIndex = random.nextInt( memberList.size() );
			member = memberList.get( randomNeighborIndex );
		}
		
		return member;
	}
	
	public void close()
	{
		shoutDown = true;
		interrupt();
	}
}