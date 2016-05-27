package distributed_fs.net.messages;

import java.io.Serializable;

public abstract class Message implements Serializable
{
	/** Type of messages. */
	public static final byte PUT				= (byte) 0x0;
	public static final byte GET				= (byte) 0x1;
	public static final byte DELETE				= (byte) 0x2;
	public static final byte GET_ALL			= (byte) 0x3;
	
	public static final byte TRANSACTION_OK		= (byte) 0x4;
	public static final byte TRANSACTION_FAILED = (byte) 0x5;
	
	public static final byte HELLO				= (byte) 0x6;
	public static final byte CLOSE				= (byte) 0x7;
	
	private static final long serialVersionUID = -145088350169009425L;
	
	/** Current type of the message. */
	private final byte opType;
	
	public Message( final byte opType )
	{
		this.opType = opType;
	}
	
	public byte getType()
	{
		return opType;
	}
}