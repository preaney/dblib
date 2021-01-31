package com.mynameis.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;

import com.mynameis.log.LogUtils;

public class DBConnection {

    private static final Logger log = LogUtils.getLogger();
    private boolean busy;
    private Connection conn;
    private final DBProperties properties;

    public DBConnection(final DBProperties props) {
        this.properties = props;
        try {
            Class.forName(props.getDriver());
            conn = DriverManager.getConnection(props.getURL(), props.getUID(), props.getPassword());
        } catch (final ClassNotFoundException e) {
            log.error("Error creating connection: " + e.getMessage(), e);
        } catch (final SQLException e) {
            log.error("Error creating connection: " + e.getMessage(), e);
        }
    }

    public DBProperties getDBProperties() {
        return this.properties;
    }

    public void dispose() {
        try {
            conn.close();
        } catch (final SQLException e) {
            log.error("Error closing connection: " + e.getMessage(), e);
            e.printStackTrace();
        }
    }

    @Override
    public void finalize() {
        this.dispose();
    }

    public Connection getConnection() {
        return conn;
    }

    public void returnConnection() {
        setBusy(false);
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(final boolean b) {
        busy = b;
    }

}
