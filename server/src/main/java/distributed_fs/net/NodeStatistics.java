/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

import java.io.Serializable;
import java.util.HashMap;

public class NodeStatistics implements Serializable
{
	private HashMap<Integer, Double> values;
	
	// Type of statistics.
	public static final int NUM_CONNECTIONS = 0;
	public static final int WORKLOAD		= 1;
	
	private static final long serialVersionUID = 7950394043516245613L;
	
	public NodeStatistics()
	{
		values = new HashMap<>();
		values.put( NUM_CONNECTIONS, 0d );
	}
	
	/**
	 * Increases the value in the specified field.
	 * 
	 * @param field
	*/
	public synchronized void increaseValue( final Integer field )
	{
		Double value = values.get( field );
		if(value == null) value = 0d;
		values.put( field, value + 1 );
	}
	
	/**
	 * Decreases the value in the specified field.
	 * 
	 * @param field
	*/
	public synchronized void decreaseValue( final Integer field )
	{
		Double value = values.get( field );
		if(value == null) value = 0d;
		values.put( field, value - 1 );
	}
	
	/**
	 * Sets a new value to the specified field.
	 * 
	 * @param field
	 * @param value
	*/
	public synchronized void setValue( final Integer field, final double value )
	{
		values.put( field, value );
	}
	
	/**
	 * Gets the value associated to the field.
	 * 
	 * @param field
	*/
	public double getValue( final int field )
	{
		return values.get( field );
	}
	
	/**
	 * Gets the average workload of the remote node.<br>
	 * The result lies between 0 and the given work load.
	*/
	public double getAverageLoad()
	{
		final double AVG_CONN = 1024;
		double numConn	   = getValue( NUM_CONNECTIONS );
		double avgWorkLoad = getValue( WORKLOAD );
		
		//System.err.println( "NumConn: " + numConn );
		//System.err.println( "AvgNumConn: " + (numConn / AVG_CONN) );
		//System.err.println( "AvgWorkLoad: " + avgWorkLoad );
		
		double workLoad = (numConn / AVG_CONN) * avgWorkLoad;
		//return Math.min( 1, workLoad );
		return workLoad;
	}
	
	@Override
	public String toString()
	{
		return values.toString();
	}
}