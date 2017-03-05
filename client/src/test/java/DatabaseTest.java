
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
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
        
        deleteDirectory( new File( "Resources" ) );
        deleteDirectory( new File( "Database" ) );
        
        DFSDatabase database = new DFSDatabase( null, null, null );
        database.newInstance();
        DistributedFile file;
        
        for(int i = 1; i <= 10; i++) {
            String fileName = "FileTest" + i + ".txt";
            assertNotNull( database.saveFile( fileName, null, new VectorClock(), false, null, false ) );
        }
        
        assertNull( database.getFile( "FileTest0.txt" ) );
        assertNotNull( file = database.getFile( "FileTest1.txt" ) );
        file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
        file.incrementVersion( "pippo" );
        assertNotNull( database.saveFile( file.getName(), null, file.getVersion(), false, null, false ) );
        
        assertNotNull( file = database.getFile( "FileTest1.txt" ) );
        file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
        file.setVersion( new VectorClock() );
        assertNull( database.saveFile( file.getName(), null, file.getVersion(), false, null, false ) );
        
        assertNull( database.getFile( "" ) );
        assertNull( database.getFile( "FileTest11.txt" ) );
        
        assertNull( database.deleteFile( file.getName(), file.getVersion(), false, null ) );
        assertNotNull( database.deleteFile( file.getName(), new VectorClock().incremented( "pippo" ).incremented( "pippo" ), false, null ) );
        assertTrue( database.getFile( file.getName() ).isDeleted() );
        
        database.close();
    }
    
    private static void deleteDirectory( final File dir )
    {
        if(dir.exists()) {
            for(File f : dir.listFiles()) {
                if(f.isDirectory())
                    deleteDirectory( f );
                f.delete();
            }
            
            dir.delete();
        }
    }
}
