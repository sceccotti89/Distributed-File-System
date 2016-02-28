
package distributed_chord.manager;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONException;

import distributed_chord.overlay.LocalChordNode;
import distributed_chord.utils.ChordUtils;

public class UpdateChordThread extends Thread
{
	/** Map of replicated virtual nodes */
	private NavigableMap<Long, LocalChordNode> virtualNodes;
	/** Atomic value used to start/stop the thread */
	private AtomicBoolean keepRunning;

	/** Time to wait for the next cycle */
	private static final int TIME = 500;

	public UpdateChordThread( final NavigableMap<Long, LocalChordNode> virtualNodes )
	{
		this.virtualNodes = virtualNodes;
		keepRunning = new AtomicBoolean( true );
		
		ChordUtils.LOGGER.info( "Active Thread successfully initialized" );
	}

	@Override
	public void run()
	{
		ChordUtils.LOGGER.info( "Active Thread launched" );
		
		while(keepRunning.get())
		{
			try
			{
				Thread.sleep( TIME );
				Iterator<Long> keys = virtualNodes.keySet().iterator();
				while(keys.hasNext())
				{
					LocalChordNode node = virtualNodes.get( keys.next() );
					
					System.out.println( "NODE ID: " + node.getID() );
					
					System.out.println( "STABILIZE..." );
					try{ node.stabilize(); }
					catch( IOException | JSONException | NotBoundException e ){}
					
					System.out.println( "FIX FINGERS..." );
					try{ node.fix_fingers(); }
					catch( IOException | JSONException | NotBoundException e ){}
					
					try{ node.printFingerTable(); }
					catch( JSONException e ){}
				}
			}
			catch( InterruptedException e )
			{
				//e.printStackTrace();
			}
		}
	}
}