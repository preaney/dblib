package com.mynameis.database;

import java.sql.Connection;

public class TestApp {

    public static void main(final String[] args) throws Exception {
        final DBConnection dbConn = ConnectionBroker.getInstance().getConnection("testdb");

        final Connection conn = dbConn.getConnection();

    }
}
