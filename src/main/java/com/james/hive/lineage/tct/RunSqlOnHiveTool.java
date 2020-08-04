package com.james.hive.lineage.tools;

import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;
import com.james.hive.lineage.tct.utils.HiveServerUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class RunSqlOnHiveTool {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        final String DB_URL = "jdbc:hive2://10.73.161.83:10000/pcg_txkd_shared_data_app";
        final String USER = "";
        final String PASS = "";

        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement stmt = conn.createStatement();

        int goodCnt = 0;
        int badCnt = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("F:\\20200723_success.sql"))) {
            String line = null;
            while (null != (line = br.readLine())) {
                String[] strArr = line.split(HiveSqlUtil.DIVIDER);
                String db = strArr[0];
                String sql = strArr[1];

                try {
                    if (db.equals("pcg_txkd_shared_data_app")) {
                        stmt.executeQuery(sql);
                        badCnt++;
                    }
                } catch (Exception e) {
                    String exMsg = e.getMessage();
                    System.out.println(exMsg);
                    System.out.println("----------------------------------------------------------------------------");
                } // try

                goodCnt++;
            }
        } catch (IOException e) {
            String exMsg = e.getMessage();
        }
    }
}
