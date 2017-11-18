/**
 * @author Stefano Ceccotti
*/

package distributed_fs.net;

public interface TransferSpeed
{
    /**
     * This method should be used to update the current state of a system.<br>
     * The parameters specify the number of bytes to receive and the current throughput
     * of the connection. A tipical utilization is when we want to estimate the time
     * of a file to be downloaded.
     * 
     * @param bytesToReceive    number of bytes to receive
     * @param throughput        the actual throughput, expressed as bytes per second
    */
    public void update( int bytesToReceive, double throughput );
}
