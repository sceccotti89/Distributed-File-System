package distributed_fs.net.messages;

import java.io.Serializable;

public abstract class Message implements Serializable
{
	private final byte opType;
	
	/** Type of messages. */
	public static final byte PUT = (byte) 0x0;
	public static final byte GET = (byte) 0x1;
	public static final byte DELETE = (byte) 0x2;
	public static final byte GET_ALL = (byte) 0x3;
	
	public static final byte TRANSACTION_OK = (byte) 0x4;
	public static final byte TRANSACTION_FAILED = (byte) 0x5;
	
	private static final long serialVersionUID = -145088350169009425L;

	public Message( final byte opType )
	{
		this.opType = opType;
	}
	
	public byte getType()
	{
		return opType;
	}
}