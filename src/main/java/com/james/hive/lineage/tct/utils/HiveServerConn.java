package com.james.hive.lineage.tct.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class HiveServerConn {
    private static final Logger LOG = LoggerFactory.getLogger(HiveServerConn.class);

    private static HiveServerConn instance = null;

    private Connection conn = null;

    private HiveServerConn() {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        final String DB_URL = "jdbc:hive2://localhost:10000/txkd_dc_insert_db";
        final String USER = "";
        final String PASS = "";

        try {
            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (Exception e) {
            String exMsg = e.getMessage();
            LOG.error("Failed to get Mysql connection: " + exMsg);
        }
    }

    public static HiveServerConn getInstance() {
        if (instance == null) {
            instance = new HiveServerConn();
        }
        return instance;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }
}
