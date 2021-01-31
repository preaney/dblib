package com.mynameis.database;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;

import com.mynameis.log.LogUtils;

public class DBProperties {

    private static Logger log = LogUtils.getLogger();
    private String driver;
    private String dbName;
    private String url;
    private String uid;
    private String password;

    private int initialConnections;

    public DBProperties() {
    }

    public DBProperties(final String filename) {
        try {
            this.load(filename);
        } catch (final java.io.IOException e) {
            log.error("Error loading dbProperties using file: " + filename, e);
        }
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(final String str) {
        driver = str;
    }

    public String getDBName() {
        return dbName;
    }

    public void setDBName(final String str) {
        dbName = str;
    }

    public String getURL() {
        return url;
    }

    public void setURL(final String str) {
        url = str;
    }

    public String getUID() {
        return uid;
    }

    public void setUID(final String str) {
        uid = str;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String str) {
        password = str;
    }

    public int getInitialConnections() {
        return initialConnections;
    }

    public void setInitialConnections(final int i) {
        initialConnections = i;
    }

    protected void load(final String filename) throws IOException {
        log.info("loading DB properties file :" + filename);
        final Properties props = new Properties();
        props.load(getClass().getResourceAsStream(filename));

        driver = props.getProperty("driver");
        dbName = props.getProperty("dbName");
        url = props.getProperty("url");
        uid = props.getProperty("uid");
        password = props.getProperty("password");
    }

    public void list(final java.io.PrintStream out) {
        out.println("DBProperties for database: " + getDBName());
        out.println("\tDriver is  " + getDriver());
        out.println("\tDB name is " + getDBName());
        out.println("\tURL is     " + getURL());
        out.println("\tUID is     " + getUID());
        out.println("\tDriver is  " + getPassword());
    }

    @Override
    public String toString() {
        return "DBProperties [driver=" + this.driver + ", dbName=" + this.dbName + ", url=" + this.url + ", uid="
                + this.uid + ", password=" + this.password + ", initialConnections=" + this.initialConnections + "]";
    }

}
