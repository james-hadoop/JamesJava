package com.james.utils;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCUtils {
    private static final Logger log = LoggerFactory.getLogger(JDBCUtils.class);

    private static BasicDataSource basicDataSource = null;

    private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
    /**
     * 配置信息
     */
    static {
        try {
            basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName("com.mysql.jdbc.Driver");
            basicDataSource.setUrl("jdbc:mysql://localhost:3306/developer");
            basicDataSource.setUsername("developer");
            basicDataSource.setPassword("developer");

            basicDataSource.setInitialSize(10);
            basicDataSource.setMaxTotal(20);
            basicDataSource.setMaxIdle(10);
            basicDataSource.setMinIdle(0);

        } catch (Exception e) {
            log.error("load mysql datasource failed ", e);
            throw e;
        }
    }

    public static DataSource getDataSource() {
        return basicDataSource;
    }

    public static Connection getConnection() throws SQLException {
        Connection conn = threadLocal.get();
        if (conn == null) {
            conn = getDataSource().getConnection();
            threadLocal.set(conn);
        }
        return conn;
    }

    public static void startTransaction() {
        try {
            Connection conn = threadLocal.get();
            if (conn == null) {
                conn = getConnection();
                threadLocal.set(conn);
            }
            conn.setAutoCommit(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void rollback() {
        try {
            Connection conn = threadLocal.get();
            if (conn != null) {
                conn.rollback();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void commit() {
        try {
            Connection conn = threadLocal.get();
            if (conn != null) {
                conn.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void close() {
        try {
            Connection conn = threadLocal.get();
            if (conn != null) {
                conn.close();
                threadLocal.remove();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}