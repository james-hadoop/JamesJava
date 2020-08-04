package com.james.hive.lineage.tct.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class HiveLineageMysqlConn {
    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageMysqlConn.class);

    private static HiveLineageMysqlConn instance = null;

    private Connection conn = null;

    private HiveLineageMysqlConn() {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String DB_URL = "jdbc:mysql://localhost:3306/developer";
        final String USER = "developer";
        final String PASS = "developer";

        try {
            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (Exception e) {
            String exMsg = e.getMessage();
            LOG.error("Failed to get Mysql connection: " + exMsg);
        }
    }

    public static HiveLineageMysqlConn getInstance() {
        if (instance == null) {
            instance = new HiveLineageMysqlConn();
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
