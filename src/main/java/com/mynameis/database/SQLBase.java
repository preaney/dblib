package com.mynameis.database;

import java.sql.*;

//import com.roc.log.*;

/**
 * A base class for database-related classes.  Contains utility methods 
 * common to many classes that deal with SQL databases.
 *
 *
 * @author Pat Reaney, April 2002
 */
public class SQLBase {

    //private static Logger log = new Logger( SQLBase.class );
    
    public SQLBase() {
    }
    

    /**
     * Returns the next available integer for use in the primary key of the given table.
     * Connects to the given database, selects the maximum value of the given primary key
     * from the given table, increments the value by 1, and returns the resulting value.
     * 
     *
     * @param   dbName  the name of the database to connect to.
     * @param   table  the table to run the select statement against
     * @param   pkName  the column name of the primary key for this table.
     * @return     an integer value one greater than the maximum primary key value in the table.
     */
    protected int getNextIndex ( String dbName, String table, String pkName ) {
        int theIndex = -1;
        DBConnection dbConn = null;
        Connection conn = null;
        Statement stmt = null;
        try{
            dbConn = ConnectionBroker.getInstance().getConnection( dbName );
            conn = dbConn.getConnection();
            stmt = conn.createStatement();
            
            String sql = "select max( "+pkName+" ) from "+table ;
            ResultSet rs = stmt.executeQuery( sql );
            if ( rs.next() ) {
                theIndex = rs.getInt( 1 );
                theIndex++;
            }
            
        } catch ( SQLException e ){
            //log.error( "there was an sql exception ", e);
            System.err.println( "there was an sql exception "+ e);
            e.printStackTrace();
           
        } catch ( UnknownDatabaseException e ){
            //log.warn( "attempt to access unknown database"+ uk.getMessage() );
            System.err.println(  "Attempt to access unknown database "+ e.getMessage() );
            e.printStackTrace();
        } finally {
            if ( dbConn != null )dbConn.returnConnection();    
            try {
                if ( stmt != null ) stmt.close();
            } catch ( SQLException e ){}
        }
        return theIndex;
    
    }
}
