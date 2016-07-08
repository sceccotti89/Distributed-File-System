
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import distributed_fs.exception.DFSException;
import distributed_fs.storage.DFSDatabase;
import distributed_fs.storage.DistributedFile;
import distributed_fs.utils.DFSUtils;
import distributed_fs.versioning.VectorClock;

public class DatabaseTest
{
    @Test
    public void testDB() throws IOException, DFSException
    {
        BasicConfigurator.configure();
        
        DFSDatabase database = new DFSDatabase( null, null, null );
		DistributedFile file;
		
		for(int i = 1; i <= 10; i++) {
		    String fileName = "Test" + i + ".txt";
			DFSUtils.existFile( database.getFileSystemRoot() + fileName, true );
		    DFSUtils.readFileFromDisk( database.getFileSystemRoot() + fileName );
			assertNotNull( database.saveFile( fileName, null, new VectorClock(), null ) );
		}
		
		assertNull( database.getFile( "Test0.txt" ) );
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
		file.incrementVersion( "pippo" );
		assertNotNull( database.saveFile( file.getName(), null, file.getVersion(), null ) );
		
		assertNotNull( file = database.getFile( "Test1.txt" ) );
		file = new DistributedFile( file.getName(), false, file.getVersion().clone(), file.getHintedHandoff() );
		file.setVersion( new VectorClock() );
		assertNull( database.saveFile( file.getName(), null, file.getVersion(), null ) );
		
		assertNull( database.getFile( "" ) );
		assertNull( database.getFile( "Test11.txt" ) );
		
		assertNull( database.removeFile( file.getName(), file.getVersion(), null ) );
		assertNotNull( database.removeFile( file.getName(), new VectorClock().incremented( "pippo" ).incremented( "pippo" ), null ) );
		assertTrue( database.getFile( file.getName() ).isDeleted() );
		
		database.close();
    }
}
