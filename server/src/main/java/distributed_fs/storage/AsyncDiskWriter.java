/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections4.map.LinkedMap;
import org.apache.log4j.Logger;

import distributed_fs.net.messages.Message;

/**
 * Class used to write asynchronously a file on disk.<br>
 * It's implemented as a queue, storing files
 * in a FIFO discipline.<br>
 * It can be disabled but the database operations would be slowed,
 * in particular for huge files.
*/
public class AsyncDiskWriter
{
    private final DBManager _db;
    private final LinkedMap<String, QueueNode> filesQueue;
    private final Map<String, Condition> activeFiles;
    private final Lock lock;
    private final Condition notEmpty;
    
    private final AtomicBoolean shutDown = new AtomicBoolean( false );
    private final AtomicBoolean toClose  = new AtomicBoolean( false );
    
    private List<AsyncDiskWriterThread> threads = null;
    private final int numThreads;
    private static final int MAX_THREADS = 2;
    
    private static final Logger LOGGER = Logger.getLogger( AsyncDiskWriter.class );
    
    
    
    
    
    
    public AsyncDiskWriter( DBManager db, boolean enableParalellWorkers )
    {
        _db = db;
        filesQueue = new LinkedMap<>( 64 );
        activeFiles = new HashMap<>( 64 );
        
        lock = new ReentrantLock();
        notEmpty = lock.newCondition();
        
        numThreads = (enableParalellWorkers) ? MAX_THREADS : 1;
        threads = new ArrayList<>( numThreads );
        for(int i = 0; i < numThreads; i++) {
            AsyncDiskWriterThread dWriter = new AsyncDiskWriterThread();
            threads.add( dWriter );
            dWriter.start();
        }
    }
    
    private class AsyncDiskWriterThread extends Thread
    {
        private QueueNode node = null;
        
        private AsyncDiskWriterThread()
        {
            setName( "AsyncWriterThread" );
        }
        
        @Override
        public void run()
        {
            LOGGER.info( "AsyncWriter thread launched." );
            
            while(!shutDown.get()) {
                try {
                    node = dequeue();
                    if(node == null)
                        continue;
                    
                    // Write or delete a file on disk.
                    if(node.opType == Message.PUT)
                        _db.writeFileOnDisk( node.path, node.file );
                    else
                        _db.deleteFileOnDisk( node.path );
                    
                    endOfWrite();
                    checkClosed();
                }
                catch( InterruptedException | IOException e ) {
                    e.printStackTrace();
                }
            }
            
            LOGGER.info( "AsyncWriter thread closed." );
        }
        
        /**
         * Method invoked by a thread that is waiting for
         * the termination of the same file on another thread.
         * 
         * @param fileName    name of file to check
        */
        private void checkActiveFile( String fileName )
        {
            if(!activeFiles.containsKey( fileName ))
                activeFiles.put( fileName, lock.newCondition() );
            else {
                try { activeFiles.get( fileName ).await(); }
                catch ( InterruptedException e ) {}
            }
        }
        
        /**
         * Signals the end of write to all the awaiting threads
         * for the current file.
        */
        private void endOfWrite()
        {
            // Notify that the file has been written on disk.
            node.lockFile.lock();
            
            node.completed = true;
            if(node.waiting)
                node.waitFile.signalAll();
            
            node.lockFile.unlock();
            
            // Notify all the other threads that the file is free.
            lock.lock();
            activeFiles.remove( node.path ).signalAll();
            lock.unlock();
        }
        
        /**
         * Removes the first element from the queue.
        */
        private QueueNode dequeue() throws InterruptedException
        {
            lock.lock();
            
            while(filesQueue.isEmpty()) {
                if(shutDown.get()) { lock.unlock(); return null; }
                notEmpty.awaitNanos( 500000 ); // 0.5 seconds.
            }
            
            // Get and remove the first element of the queue.
            QueueNode node = filesQueue.remove( 0 );
            checkActiveFile( node.path );
            
            lock.unlock();
            
            return node;
        }
        
        /**
         * Checks whether a file has been written into disk or not.
         * In the latter case a waiting operation is performed,
         * remaining in this situation until the request has not been completed.
         * 
         * @param fileName     name of the file
         * 
         * @return {@code true} if the waiting phase has been performed,
         *         {@code false} otherwise
        */
        private boolean checkWrittenFile( String fileName )
        {
            boolean hasWaited = false;
            QueueNode qNode = null;
            
            if(node != null) {
                node.lockFile.lock();
                if(node.path.equals( fileName ) && !node.completed) {
                    qNode = node;
                    qNode.waiting = true;
                }
                node.lockFile.unlock();
            }
            
            if(qNode == null) {
                lock.lock();
                
                // Check whether the requested file is in the queue.
                int index = filesQueue.indexOf( fileName );
                if(index >= 0) {
                    (qNode = filesQueue.getValue( index )).waiting = true;
                    
                    // To reduce the waiting time,
                    // remove all the previous not marked elements
                    // and put them on the last positions.
                    for(int i = index - 1; i >= 0; i--) {
                        if(filesQueue.getValue( i ).waiting)
                            break;
                        else {
                            QueueNode node = filesQueue.remove( i );
                            filesQueue.put( node.path, node );
                        }
                    }
                }
                
                lock.unlock();
            }
            
            if(qNode != null) {
                hasWaited = true;
                
                qNode.lockFile.lock();
                
                // Wait for the completion of the file.
                if(!qNode.completed) {
                    try { qNode.waitFile.await(); }
                    catch( InterruptedException e ) {}
                }
                
                qNode.lockFile.unlock();
            }
            
            return hasWaited;
        }
        
        private void checkClosed()
        {
            lock.lock();
            
            // If empty and closed, starts the shutdown.
            if(toClose.get() && filesQueue.isEmpty()) {
                shutDown.set( true );
                toClose.set( false );
            }
            
            lock.unlock();
        }
    }
    
    /**
     * Inserts a file into the queue.
     * 
     * @param file     the file's content. It may be {@code null} in a delete operation
     * @param path     the file's location. It may be a relative or absolute path to the file
     * @param opType   the operation to perform ({@link Message#PUT} or {@link Message#DELETE})
    */
    public void enqueue( byte[] file, String path, byte opType )
    {
        lock.lock();
        
        filesQueue.put( path, new QueueNode( file, path, opType ) );
        if(filesQueue.size() == 1)
            notEmpty.signal();
        
        lock.unlock();
    }

    /**
     * Checks whether a file has been written into disk or not.
     * In the latter case a waiting operation is performed,
     * remaining in this situation until the request has not been completed.
     * 
     * @param fileName     name of the file
    */
    public void checkWrittenFile( String fileName )
    {
        for(int i = 0; i < numThreads; i++) {
            if(threads.get( i ).checkWrittenFile( fileName ))
                break;
        }
    }
    
    /**
     * Closes the thread leaving it for the termination
     * of the pending operations.
    */
    public void setClosed()
    {
        lock.lock();
        toClose.set( true );
        lock.unlock();
    }
    
    /**
     * Closes the thread without waiting for the termination
     * of the pending operations.
    */
    public void shutDown()
    {
        shutDown.set( true );
        
        for(int i = 0; i < numThreads; i++) {
            try { threads.get( i ).join(); }
            catch( InterruptedException e ) {}
        }
    }
    
    private class QueueNode
    {
        public final byte[] file;
        public final String path;
        public final byte opType;
        public boolean waiting;
        public boolean completed;
        
        // Lock for the access of the file.
        public final ReentrantLock lockFile = new ReentrantLock();
        public final Condition waitFile = lockFile.newCondition();
        
        public QueueNode( byte[] file, String path, byte opType )
        {
            this.file = file;
            this.path = path;
            this.opType = opType;
        }
    }
}
