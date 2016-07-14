/**
 * @author Stefano Ceccotti
*/

package distributed_fs.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DBManager
{
    private List<DBListener> listeners = null;
    
    protected boolean disableAsyncWrites = false;
    protected boolean disableReconciliation = false;
    
    protected AtomicBoolean shutDown = new AtomicBoolean( false );
    
    public DBManager() {}
    
    /**
     * By default all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     * <p/>
     * It is possible to disable Background Writer Thread, but this greatly hurts concurrency.
     * Without async writes, all threads remain blocked until all previous writes are not finished (single big lock).
     *
     * <p/>
     * This may workaround some problems.
     *
    */
    public void disableAsyncWrites() {
        disableAsyncWrites = true;
    }
    
    /**
     * Disable the conflict resolutor
     * when two files have concurrent versions.
    */
    public void disableResolveConflicts() {
        disableReconciliation = true;
    }
    
    /**
     * Adds a new database listener.
    */
    public void addListener( final DBListener listener )
    {
        if(listener != null) {
            if(listeners == null)
                listeners = new ArrayList<>();
            listeners.add( listener );
        }
    }
    
    /**
     * Removes the given listener.
    */
    public void removeListener( final DBListener listener )
    {
        if(listener != null && listeners != null)
            listeners.remove( listener );
    }
    
    /**
     * Notifies all the attached listeners about the last operation done.
     * 
     * @param fileName     name of the file
     * @param operation    type of operation ({@code GET} or {@code DELETE})
    */
    protected void notifyListeners( final String fileName, final byte operation )
    {
        if(listeners != null) {
            for(DBListener listener : listeners)
                listener.dbEvent( fileName, operation );
        }
    }
    
    public static interface DBListener
    {
        public void dbEvent( final String fileName, final byte code );
    }
}