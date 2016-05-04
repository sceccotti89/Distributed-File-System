/**
 * @author Stefano Ceccotti
*/

package distributed_fs.files;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import distributed_fs.utils.Utils;

public class VersioningDatabase
{
	private final Connection conn;
	
	public VersioningDatabase( final String dbFileName ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		Class.forName( "org.hsqldb.jdbcDriver" ).newInstance();
		
		Utils.createDirectory( Utils.VERSIONS_LOCATION );
		
		// Connect to the database. This will load the db files and start the
		// database if it is not alread running.
		// db_file_name_prefix is used to open or create files that hold the state
		// of the db.
		// It can contain directory names relative to the
		// current working directory.
		// The password can be changed when the database is created for the first time.
		conn = DriverManager.getConnection( "jdbc:hsqldb:" + dbFileName, "cecco", "ste" );
		
		try {
			// make an empty table
			// by declaring the column IDENTITY, the db will automatically
			// generate unique values for new rows - useful for row keys
			update( "CREATE TABLE Versions ( id INTEGER IDENTITY, "
											+ "fileId VARCHAR(256), "
											+ "vClock VARCHAR(1024), "
											+ "hintedHandoff VARCHAR(256), "
											+ "Deleted INTEGER, "
											+ "TTL INTEGER )" );
		} catch( SQLException ex2 ) {
			// Ignore.
			//ex2.printStackTrace();  // Second time we run program
									  // should throw exception since table
									  // already there.
									  // This will have no effect on the db.
		}
	}
	
	/**
	 * Used for SQL commands CREATE, DROP, INSERT and UPDATE.
	 * 
	 * @param expression	the query
	*/
	public void update( final String expression ) throws SQLException
	{
		Statement st = conn.createStatement();
		
		if(st.executeUpdate( expression ) == -1)
			System.out.println( "Database error : " + expression );
		
		st.close();
	}
	
	/**
	 * Used for SQL command SELECT.
	 * 
	 * @param expression	the query
	 * @param columnIndex	column from where the value is retrieved
	*/
	public List<String> query( final String expression, final int columnIndex ) throws SQLException
	{
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery( expression );
		
		List<String> values = getValues( rs, columnIndex );
		st.close();    // NOTE!! if you close a statement the associated ResultSet is
					   // closed too
					   // so you should copy the contents to some other object.
					   // the result set is invalidated also if you recycle a Statement
					   // and try to execute some other query before the result set has been
					   // completely examined.
		
		return values;
	}
	
	/**
	 * Returns the value specified in the index column
	 * 
	 * @param rs			contains the result of the query
	 * @param columnIndex	column from where the value is retrieved
	*/
	private List<String> getValues( final ResultSet rs, final int columnIndex ) throws SQLException
	{
		// the order of the rows in a cursor
		// are implementation dependent unless you use the SQL ORDER statement
		ResultSetMetaData meta = rs.getMetaData();
		int colmax = meta.getColumnCount();
		List<String> values = new ArrayList<>();
		
		// the result set is a cursor into the data.  You can only
		// point to one row at a time
		// assume we are pointing to BEFORE the first row
		// rs.next() points to next row and returns true
		// or false if there is no next row, which breaks the loop
		while(rs.next()) {
			// if there are multiple versions, save the last one
			for(int i = 0; i < colmax; i++) {
				// In SQL the first column is indexed with 1 not 0
				if(i < values.size())
					values.set( i, rs.getString( i + 1 ) );
				else
					values.add( rs.getString( i + 1 ) );
			}
		}
		
		return values;
	}
	
	public void shutdown()
	{
		try {
			Statement st = conn.createStatement();
			
			// db writes out to files and performs clean shuts down
			// otherwise there will be an unclean shutdown
			// when program ends
			st.execute( "SHUTDOWN" );
			conn.close();    // if there are no other open connections
		}
	    catch( SQLException e ){}
	}
}