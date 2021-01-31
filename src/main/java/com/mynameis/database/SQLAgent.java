package com.mynameis.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;

import com.mynameis.log.LogUtils;

public class SQLAgent {

    private static Logger log = LogUtils.getLogger();

    private String sql;
    private String database;

    private DBConnection dbConn;
    private Connection conn;
    private Statement stmt;
    private ResultSet rs;

    public SQLAgent() {
    }

    public SQLAgent(final String database, final String sql) {
        this.database = database;
        this.sql = sql;
    }

    public boolean execute() throws SQLException, UnknownDatabaseException {
        dbConn = ConnectionBroker.getInstance().getConnection(database);
        conn = dbConn.getConnection();
        stmt = conn.createStatement();

        final boolean b = stmt.execute(sql);

        return b;
    }

    public boolean execute(final String database, final String sql) throws SQLException, UnknownDatabaseException {
        this.database = database;
        this.sql = sql;
        return execute();
    }

    public ResultSet query(final String database, final String sql) throws SQLException, UnknownDatabaseException {
        this.database = database;
        this.sql = sql;
        return query();
    }

    public ResultSet query() throws SQLException, UnknownDatabaseException {
        dbConn = ConnectionBroker.getInstance().getConnection(database);
        conn = dbConn.getConnection();
        stmt = conn.createStatement();

        rs = stmt.executeQuery(sql);

        return rs;
    }

    public void dispose() {
        try {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (dbConn != null)
                ConnectionBroker.getInstance().returnConnection(dbConn);
        } catch (final SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void main(final String[] args) {
        SQLAgent agent = null;
        try {
            agent = new SQLAgent("weddingography", "select photo_id from photo");
            final ResultSet rs = agent.query();
            while (rs.next()) {
                System.out.println("photo_id " + rs.getString(1));
            }
        } catch (final Exception e) {
            System.err.println("error " + e + e.getMessage());
        } finally {
            agent.dispose();
        }
    }

    /**
     * Returns the next available integer for use in the primary key of the given
     * table. Connects to the given database, selects the maximum value of the given
     * primary key from the given table, increments the value by 1, and returns the
     * resulting value.
     * 
     *
     * @param dbName
     *            the name of the database to connect to.
     * @param table
     *            the table to run the select statement against
     * @param pkName
     *            the column name of the primary key for this table.
     * @return an integer value one greater than the maximum primary key value in
     *         the table.
     */
    public static int getNextIndex(final String dbName, final String table, final String pkName) {
        int theIndex = -1;
        DBConnection dbConn = null;
        Connection conn = null;
        Statement stmt = null;
        try {
            dbConn = ConnectionBroker.getInstance().getConnection(dbName);
            conn = dbConn.getConnection();
            stmt = conn.createStatement();

            final String sql = "select max( " + pkName + " ) from " + table;
            final ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                theIndex = rs.getInt(1);
                theIndex++;
            }

        } catch (final SQLException e) {
            // log.error( "there was an sql exception ", e);
            System.err.println("there was an sql exception " + e);
            e.printStackTrace();

        } catch (final UnknownDatabaseException e) {
            // log.warn( "attempt to access unknown database"+ uk.getMessage() );
            System.err.println("Attempt to access unknown database " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (dbConn != null)
                dbConn.returnConnection();
            try {
                if (stmt != null)
                    stmt.close();
            } catch (final SQLException e) {
            }
        }
        return theIndex;

    }

    public static boolean isUnique(final String dbName, final String tableName, final String columnName,
            final Object value) {
        boolean unique = false;

        DBConnection dbConn = null;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        // PUT SINGLE QUOTES AROUND THE VALUE IF IT IS NOT NUMERIC.
        String valStr = value.toString().trim();
        try {
            final double d = Double.parseDouble(valStr);
        } catch (final NumberFormatException e) {
            valStr = "'" + valStr + "'";
        }

        try {
            dbConn = ConnectionBroker.getInstance().getConnection(dbName);
            conn = dbConn.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(
                    "select " + columnName + " from " + tableName + " where " + columnName + "=" + valStr);

            // if rs.next() is true, then unique is false
            unique = !(rs.next());

        } catch (final SQLException e) {
            log.error("there was an sql exception ", e);
            e.printStackTrace();
        } catch (final UnknownDatabaseException uk) {
            log.error("attempt to access unknown database", uk);
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (dbConn != null)
                    dbConn.returnConnection();
            } catch (final Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return unique;
    }

    public Connection getConnection() {
        return this.conn;
    }

    public String getDatabase() {
        return this.database;
    }

    public DBConnection getDBConnection() {
        return this.dbConn;
    }

    public Statement getStmt() {
        return this.stmt;
    }

}
