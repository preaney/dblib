package com.mynameis.database;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import com.mynameis.log.LogUtils;

public class ConnectionBroker {

    // had to do this for webfactional db, because the conection kept getting
    // broken.
    // Close any connection when it is returned and remove from the pool.
    private static final boolean DISPOSE_ON_RETURN = true;

    /** Singleton pattern - only one instance of this class can exist */
    protected static ConnectionBroker instance = new ConnectionBroker();

    public static ConnectionBroker getInstance() {
        return instance;
    }

    // private static Category log = Category.getInstance(
    // ConnectionBroker.class.getName() );
    private static Logger log = LogUtils.getLogger();

    /** Keys: database names. Values: Vectors of connections. */
    // protected Hashtable connectionPool;
    private final Map<String, List<DBConnection>> connectionPool;// dbNameToConnection;
    /** Keys: database names. Values: DBProperties objects. */

    // protected Hashtable dbTable;

    private final Map<String, DBProperties> dbTable;

    private ConnectionBroker() {
        dbTable = this.createDBTable();
        connectionPool = new HashMap<String, List<DBConnection>>();
    }

    public synchronized DBConnection getConnection(final String database)
            throws com.mynameis.database.UnknownDatabaseException {
        // look up database in db, get vector of connections
        // select connection if available, otherwise create new and add to
        // Vector
        // return connection.
        log.debug("ConnectionBroker: request for connection to database: " + database);
        // new Throwable().printStackTrace();
        final DBProperties dbProps = dbTable.get(database);
        if (dbProps == null) {
            this.printDBNames();
            throw new com.mynameis.database.UnknownDatabaseException("Unknown database: " + database);
        }
        DBConnection dbConn = null;
        List<DBConnection> pool = connectionPool.get(database);
        // Vector pool = (Vector)connectionPool.get( database );

        if (pool == null) {
            pool = new LinkedList<DBConnection>();
            connectionPool.put(database, pool);
            dbConn = new DBConnection(dbProps);
            pool.add(dbConn);
            log.debug("ConnectionBroker: no connections in pool " + database + ", creating new... ");
        } else {
            final int count = pool.size();
            // get the first available connection
            for (int i = 0; i < count && dbConn == null; i++) {
                final DBConnection c = pool.get(i);
                if (c != null && !c.isBusy()) {
                    dbConn = c;
                }
            }
            // none available? make one.
            if (dbConn == null) {
                dbConn = new DBConnection(dbProps);
                pool.add(dbConn);
                log.debug("ConnectionBroker: all connections in pool busy, creating new... ");
            } else { // debug - remove later
                log.debug("ConnectionBroker: got connection from pool ");
            }
        }
        log.debug("Number of connections in pool for " + database + ": " + pool.size());
        dbConn.setBusy(true);
        return dbConn;
    }

    public synchronized void returnConnection(final DBConnection conn) {
        // set DBConnection.isBusy to false
        if (DISPOSE_ON_RETURN) {
            conn.dispose();
            remove(conn);
        } else {
            conn.setBusy(false);
        }
    }

    private synchronized boolean remove(final DBConnection conn) {
        final String dbName = conn.getDBProperties().getDBName();
        final List<DBConnection> connections = connectionPool.get(dbName);
        if (connections != null) {
            final boolean removed = connections.remove(conn);
            log.debug(
                    "Removing connection for db " + dbName + ". Number of connections in pool : " + connections.size());
            return removed;
        }
        return false;
    }

    public Map<String, DBProperties> createDBTable() {
        // read properties file w/list of db config files
        // create DBProperties objects and add to Hashtable
        final String filename = "MasterDB.properties";
        final Map<String, DBProperties> table = new HashMap<String, DBProperties>();
        try {
            final InputStream is = getClass().getResourceAsStream(filename);

            final Properties props = new Properties();
            props.load(is);

            final Enumeration propNames = props.propertyNames();
            while (propNames.hasMoreElements()) {
                final String dbName = (String) propNames.nextElement();
                final String dbFile = props.getProperty(dbName);
                // log.debug( "Creating DBProperties object for database :" +
                // dbName );
                // log.debug( "Using properties file: " + dbFile );
                System.out.println("Creating DBProperties object for database :" + dbName);
                System.out.println("Using properties file: " + dbFile);
                final DBProperties dbProps = new DBProperties(dbFile);

                table.put(dbProps.getDBName(), dbProps);
            }
        } catch (final java.io.IOException e) {
            System.err.println("Error creating DBTable using file: " + filename);
            System.err.println(e);
            e.printStackTrace();
        }
        return table;

    }

    public void printDBNames() {
        log.debug("size of db table is " + dbTable.size());
        for (final String dbName : dbTable.keySet()) {
            final DBProperties dbProps = dbTable.get(dbName);
            log.debug("DBName: " + dbName + ". properties: " + dbProps.toString());
        }

        // Enumeration propNames = dbTable.elements();
        // //log.debug( "Database names in table: " );
        // while ( propNames.hasMoreElements() ){
        // DBProperties dbProps = (DBProperties)propNames.nextElement();
        // //log.debug( dbProps.getDBName() );
        // }
    }
}
