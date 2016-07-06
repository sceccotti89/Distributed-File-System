
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import distributed_fs.exception.DFSException;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.versioning.VectorClock;

public class DatabaseTest
{
    @Test
    public void testDB() throws IOException, DFSException
    {
        BasicConfigurator.configure();
        
        DFSDatabase database = new DFSDatabase( null, null, null );
        DistributedFile file;
        
        // Add some files...
        for(int i = 1; i <= 10; i++)
            assertNotNull( database.saveFile( "Test" + i + ".txt", null, new VectorClock(), null, false ) );
        
        // Get and Update tests...
        assertNull( database.getFile( "Test0.txt" ) );
        assertNotNull( file = database.getFile( "Test1.txt" ) );
        file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
        file.incrementVersion( "pippo" );
        assertNotNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
        
        assertNotNull( file = database.getFile( "Test1.txt" ) );
        file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
        file.setVersion( new VectorClock() );
        assertNull( database.saveFile( file.getName(), null, file.getVersion(), null, false ) );
        
        assertNull( database.getFile( "" ) );
        assertNull( database.getFile( "Test11.txt" ) );
        
        // Delete tests...
        assertNull( database.removeFile( file.getName(), file.getVersion(), null ) );
        assertNotNull( database.removeFile( file.getName(), new VectorClock().incremented( "pippo" ).incremented( "pippo" ), null ) );
        assertTrue( database.getFile( file.getName() ).isDeleted() );
        
        database.close();
    }
}