package com.james.hive.lineage.tct.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

public class HiveServerUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveServerUtil.class);

    public static void execDdl(String sql) {
        if (null == sql || sql.isEmpty()) {
            return;
        }

        Connection conn = HiveServerConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get HiveServer connection.");
        }

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (Exception ex) {
            String exMsg = ex.getMessage();
            LOG.error(exMsg);
        }
        if (null == stmt) {
            return;
        }

        String[] sqlArr = sql.split(";");

        for (String s : sqlArr) {
            try {
                stmt.executeQuery(s);
                stmt.closeOnCompletion();
            } catch (Exception e) {
                // e.printStackTrace();
                String ex_msg = e.getMessage();
                LOG.error(ex_msg);
            }
        } // for

        return;
    }
}
