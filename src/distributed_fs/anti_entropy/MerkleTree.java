/**
 * @author Stefano Ceccotti
*/

package distributed_fs.anti_entropy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import distributed_fs.utils.Utils;

/**
 * MerkleTree is an implementation of a Merkle binary hash tree where the leaves
 * are signatures (hashes, digests, CRCs, etc.) of some underlying data structure
 * that is not explicitly part of the tree.
 * 
 * The internal leaves of the tree are signatures of its two child nodes. If an
 * internal node has only one child, then the signature of the child node is
 * adopted ("promoted").
 * 
 * MerkleTree knows how to serialize itself to a binary format, but does not
 * implement the Java Serializer interface.  The {@link #serialize()} method
 * returns a byte array, which should be passed to 
 * {@link MerkleDeserializer#deserialize(byte[])} in order to hydrate into
 * a MerkleTree in memory.
 * 
 * This MerkleTree is intentionally ignorant of the hashing/checksum algorithm
 * used to generate the leaf signatures. It uses an hash function provided by the user,
 * to generate signatures for all internal node signatures (other than those "promoted"
 * that have only one child).
 * 
 * The Hash function is cryptographically secure, so this implementation
 * should be used in scenarios where the data is being received from
 * an untrusted source. If the speed is the main priority, the Adler32 CRC is a better solution
 * to achieve it.
*/
public class MerkleTree
{
	public static final int MAGIC_HDR = 0xcdaace99;
	private static final int INT_BYTES = Integer.BYTES;
	private static final int LONG_BYTES = Long.BYTES;
	public static final byte LEAF_SIG_TYPE = 0x0;
	private static final byte INTERNAL_SIG_TYPE = 0x01;
	
	public static final HashFunction _hash = Hashing.md5();
	private List<byte[]> leafSigs;
	private Node root;
	private int depth;
	private int nnodes;
	private int numLeaves;
	
	/**
	 * Use this constructor to create a MerkleTree from a list of leaf keys, expressed in bytes.
	 * The Merkle tree is built from the bottom up.
	 * 
	 * @param leafKeys		byte representation of the files
	*/
	public MerkleTree( final List<byte[]> leafKeys )
	{
		int size = leafKeys.size();
		List<byte[]> leafSignatures = new ArrayList<>( size );
		for(byte[] bytes : leafKeys)
			leafSignatures.add( leaveHash( bytes ) );
		
		constructTree( leafSignatures );
	}
	
	/**
	 * Use this constructor when you have already constructed the tree of Nodes (from deserialization).
	 * 
	 * @param treeRoot
	 * @param numNodes
	 * @param height
	 * @param leafSignatures
	 */
	public MerkleTree( final Node treeRoot, final int numNodes, final int height, final List<byte[]> leafSignatures)
	{
		root = treeRoot;
		nnodes = numNodes;
		depth = height;
		leafSigs = leafSignatures;
	}
	
	/**
	 * Serialization format:
	 * (magicheader:int)(numnodes:int)[(nodetype:byte)(siglength:int)(signature:byte[])]
	 * 
	 * @return
	*/
	public byte[] serialize()
	{
		int magicHeaderSz = INT_BYTES;
		int nnodesSz = INT_BYTES;
		int hdrSz = magicHeaderSz + nnodesSz;
		
		int typeByteSz = 1;
		int siglength = INT_BYTES;
		
		int parentSigSz = LONG_BYTES;
		int leafSigSz = leafSigs.get( 0 ).length;
		
		// some of the internal nodes may use leaf signatures (when "promoted")
		// so ensure that the ByteBuffer overestimates how much space is needed
		// since ByteBuffer does not expand on demand
		int maxSigSz = leafSigSz;
		if (parentSigSz > maxSigSz) {
			maxSigSz = parentSigSz;
		}
		
		int spaceForNodes = (typeByteSz + siglength + maxSigSz) * nnodes; 
		
		int cap = hdrSz + spaceForNodes;
		ByteBuffer buf = ByteBuffer.allocate( cap );
		
		buf.putInt( MAGIC_HDR ).putInt( nnodes );  // header
		serializeBreadthFirst( buf );

		// the ByteBuf allocated space is likely more than was needed
		// so copy to a byte array of the exact size necesssary
		byte[] serializedTree = new byte[buf.position()];
		buf.rewind();
		buf.get( serializedTree );
		
		return serializedTree;
	}
	
	/**
	 * Serialization format after the header section:
	 * [(nodetype:byte)(siglength:int)(signature:[]byte)]
	 * 
	 * @param buf
	*/
	private void serializeBreadthFirst( final ByteBuffer buf )
	{
		Queue<Node> q = new ArrayDeque<Node>( (nnodes / 2) + 1 );
		q.add( root );
		
		while(!q.isEmpty()) {
			Node nd = q.remove();
			buf.put( nd.type ).putInt( nd.sig.length ).put( nd.sig );
			
			if(nd.left != null)
				q.add( nd.left );
			
			if(nd.right != null)
				q.add( nd.right );
		}
	}
	
	/**
	 * Create a tree from the bottom up starting from the leaf signatures.
	 * 
	 * @param signatures
	*/
	private void constructTree( final List<byte[]> signatures )
	{
		//if (signatures.size() <= 1) {
			//throw new IllegalArgumentException( "Must be at least two signatures to construct a Merkle tree" );
		//}
		
		leafSigs = signatures;
		nnodes = signatures.size();
		depth = 1;
		
		if(nnodes == 1) {
			Node leaf = constructLeafNode( signatures.get( 0 ), 0 );
			root = constructInternalNode( leaf, null );
			nnodes = 2;
			numLeaves = 1;
		}
		else
		{
			List<Node> parents = bottomLevel( signatures );
			nnodes += parents.size();
			while (parents.size() > 1) {
				parents = internalLevel( parents );
				depth++;
				nnodes += parents.size();
			}
			
			root = parents.get( 0 );
		}
	}
	
	public int getNumNodes()
	{
		return nnodes;
	}
	
	public Node getRoot()
	{
		return root;
	}
	
	public int getHeight()
	{
		return depth;
	}
	
	public int getNumLeaves()
	{
		return numLeaves;
	}
	
	/**
	 * Returns the leaves reachable from the given node.
	 * 
	 * @param n		the starting node
	 * 
	 * @return list of leaves reached from the given node
	*/
	public LinkedList<Node> getLeavesFrom( final Node n )
	{
		LinkedList<Node> leaves = new LinkedList<>();
		List<Node> nodes = new LinkedList<>();
		nodes.add( n );
		
		while(nodes.size() > 0) {
			Node node = nodes.remove( 0 );
			if(node.type == LEAF_SIG_TYPE)
				leaves.add( node );
			else {
				if(node.left != null) nodes.add( node.left );
				if(node.right != null) nodes.add( node.right );
			}
		}
		
		return leaves;
	}
	
	/**
	 * Constructs an internal level of the tree
	*/
	private List<Node> internalLevel( final List<Node> children )
	{
		List<Node> parents = new ArrayList<>( children.size() / 2 );
		
		for (int i = 0; i < children.size() - 1; i += 2) {
			Node child1 = children.get( i );
			Node child2 = children.get( i+1 );
			
			Node parent = constructInternalNode( child1, child2 );
			parents.add(parent);
		}
		
		if (children.size() % 2 != 0) {
			Node child = children.get( children.size() - 1 );
			Node parent = constructInternalNode( child, null );
			parents.add( parent );
		}
		
		return parents;
	}
	
	/**
	 * Constructs the bottom part of the tree - the leaf nodes and their
	 * immediate parents.  Returns a list of the parent nodes.
	*/
	private List<Node> bottomLevel( final List<byte[]> signatures )
	{
		List<Node> parents = new ArrayList<Node>( signatures.size() / 2 );
		int position = 0;
		
		for (int i = 0; i < signatures.size() - 1; i += 2) {
			Node leaf1 = constructLeafNode( signatures.get( i ), position++ );
			Node leaf2 = constructLeafNode( signatures.get( i+1 ), position++ );
			numLeaves += 2;
			
			Node parent = constructInternalNode( leaf1, leaf2 );
			parents.add( parent );
		}
		
		// if odd number of leafs, handle last entry
		if (signatures.size() % 2 != 0) {
			Node leaf = constructLeafNode( signatures.get( signatures.size() - 1 ), position++ );      
			Node parent = constructInternalNode( leaf, null );
			parents.add( parent );
			numLeaves++;
		}
		
		return parents;
	}
	
	private Node constructInternalNode( final Node child1, final Node child2 )
	{
		Node parent = new Node();
		parent.type = INTERNAL_SIG_TYPE;
		
		if (child2 == null) {
			parent.sig = child1.sig;
		} else {
			parent.sig = internalHash( child1.sig, child2.sig );
		}
		
		parent.left = child1;
		parent.right = child2;
		return parent;
	}
	
	private Node constructLeafNode( final byte[] signature, final int position )
	{
		Node leaf = new Node();
		leaf.type = LEAF_SIG_TYPE;
		leaf.sig = signature;
		leaf.position = position;
		return leaf;
	}
	
	private byte[] internalHash( final byte[] leftChildSig, final byte[] rightChildSig )
	{
		String leftSig = Utils.bytesToHex( leftChildSig );
		String rightSig = Utils.bytesToHex( rightChildSig );
		//System.out.println( "LEFT: " + leftSig );
		//System.out.println( "RIGHT: " + rightSig );
		//System.out.println( "RESULT: " + Utils.bytesToHex( _hash.hashBytes( (leftSig + rightSig).getBytes() ).asBytes() ) );
		return _hash.hashBytes( (leftSig + rightSig).getBytes() ).asBytes();
	}
	
	private byte[] leaveHash( final byte[] leaveSig )
	{
		return getSignature( leaveSig );
	}
	
	/**
	 * Returns the signature of an object.
	 * 
	 * @param object		the byte array of an object
	*/
	public static byte[] getSignature( final byte[] object )
	{
		return _hash.hashBytes( object ).asBytes();
	}
	
	
	/* ---[ Node class ]--- */
	
	/**
	 * The Node class should be treated as immutable, though immutable
	 * is not enforced in the current design.
	 * 
	 * A Node knows whether it is an internal or leaf node and its signature.
	 * 
	 * Internal Nodes will have at least one child (always on the left).
	 * Leaf Nodes will have no children (left = right = null).
	*/
	public static class Node
	{
		public byte type;  // INTERNAL_SIG_TYPE or LEAF_SIG_TYPE
		public byte[] sig; // signature of the node
		public Node left;
		public Node right;
		public int position;
		
		@Override
		public String toString()
		{
			String leftType = "<null>";
			String rightType = "<null>";
			if (left != null) {
				leftType = String.valueOf( left.type );
			}
			if (right != null) {
				rightType = String.valueOf( right.type );
			}
			
			return String.format( "MerkleTree.Node<type:%d, sig:[%s], left (type): %s, right (type): %s>",
								  type, Utils.bytesToHex( sig ), leftType, rightType );
		}
	}
}