package com.mynameis.database;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BatchUpload {
    protected String file;
    protected String database;

    protected BufferedReader reader;
    protected DBConnection dbConn;
    protected Connection conn;
    protected ConnectionBroker broker;

    public BatchUpload(final String f, final String db) {
        file = f;
        database = db;
        this.connect();
    }

    public void upload() throws Exception {
        this.upload(file, database);
    }

    public void upload(final String f, final String db) throws Exception {
        file = f;
        database = db;
        if (file == null || database == null) {
            throw new Exception("File or database was null.  File = " + file + ", Database = " + database);
        }
        if (conn == null) {
            this.connect();
        }
        conn.setAutoCommit(false);
        final Statement stmt = conn.createStatement();
        reader = this.openFile(f);
        String line = null;
        int count = 1;
        try {
            while ((line = reader.readLine()) != null) {
                System.out.println("Processing line number: " + count++);
                stmt.execute(line);
            }
            conn.commit();
        } catch (final SQLException e) {
            System.out.println("An Exception occurred: " + e);
            e.printStackTrace();
            try {
                System.out.println("Rolling back transaction...");
                conn.rollback();
            } catch (final SQLException ex) {
                System.out.println("Error performing rollback: " + ex);
            }
        } finally {
            if (dbConn != null) {
                broker.returnConnection(dbConn);
                dbConn.dispose();
            }
        }
    }

    protected void connect() {
        broker = ConnectionBroker.getInstance();
        try {
            dbConn = broker.getConnection(database);
            conn = dbConn.getConnection();
        } catch (final UnknownDatabaseException e) {
            System.out.println("Unknown Database: " + database);
            e.printStackTrace();
        }
    }

    protected BufferedReader openFile(final String filename) throws IOException, FileNotFoundException {
        return new BufferedReader(new FileReader(filename));
    }
}
