package com.mynameis.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;

import com.mynameis.util.log.LogUtils;

public class TestApp {

    static {
        LogUtils.configureLogging();
    }

    private static final Logger log = LogUtils.getLogger();

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
            log.info("Column {} = {} ", col, valueStr);
        }
    }
}
