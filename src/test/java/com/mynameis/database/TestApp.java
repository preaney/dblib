package com.mynameis.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestApp {

    private static final String[] columns = {
            "address_id",
            "street",
            "street2",
            "city",
            "state",
            "zip",
            "phone",
            "email" };

    public static void main(final String[] args) throws Exception {
        final DBConnection dbConn = ConnectionBroker.getInstance().getConnection("test");

        final Connection conn = dbConn.getConnection();

        final Statement stmt = conn.createStatement();

        final ResultSet rs = stmt.executeQuery("select * from address");

        while (rs.next()) {
            printResult(rs, columns);
        }
    }

    private static void printResult(final ResultSet rs, final String[] columns) throws SQLException {
        for (final String col : columns) {
            final Object value = rs.getObject(col);
            final String valueStr = value == null ? "" : value.toString();
            System.out.printf("Column %s = %s \n", col, valueStr);
        }
    }
}
