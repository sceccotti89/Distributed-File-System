/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.IOException;
import java.util.List;

import distributed_fs.net.Networking.Session;
import distributed_fs.overlay.manager.QuorumThread.QuorumNode;

public interface FileTransfer
{
    /** 
     * Sends the list of files to the destination address.
     * 
     * @param address           destination IP address
     * @param port              destination port
     * @param files             list of files
     * @param wait_response     {@code true} if the process have to wait the response, {@code false} otherwise
     * @param synchNodeId       identifier of the synchronizing node (used during the anti-entropy phase)
     * @param node              the quorum node object. Used to release a file after the transmission
     * 
     * @return {@code true} if the files are successfully transmitted, {@code false} otherwise
    */
    public boolean sendFiles( String address, int port, List<DistributedFile> files,
                              boolean wait_response, String synchNodeId, QuorumNode node );
    
    /** 
     * Reads the incoming files and apply the appropriate operation,
     * based on file's deleted bit.
     * 
     * @param session   current TCP session
    */
    public void receiveFiles( Session session ) throws IOException, InterruptedException;
}
