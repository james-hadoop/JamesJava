package com.james.hive.lineage.tools;

import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;
import com.james.hive.lineage.tct.utils.HiveServerUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.BufferedReader;
<<<<<<< HEAD
import java.io.FileReader;
=======
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
>>>>>>> ded0ccbf23b714c015759956328e4984bcbf7dae
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
<<<<<<< HEAD
import java.util.List;
=======
import java.util.HashMap;
import java.util.Map;
>>>>>>> ded0ccbf23b714c015759956328e4984bcbf7dae

public class RunSqlOnHiveTool {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
<<<<<<< HEAD
        final String DB_URL = "jdbc:hive2://9.134.122.115:10000/pcg_txkd_shared_data_app";
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
=======
        final String DB_URL_PREFIX = "jdbc:hive2://localhost:10000/";
        final String USER = "";
        final String PASS = "";
        final String GOOD_SQL_PATH = "/home/james/_AllDocMap/hive_lineage_parser/good_sql_file.sql";
        final String BAD_SQL_PATH = "/home/james/_AllDocMap/hive_lineage_parser/bad_sql_file.sql";

        BufferedWriter bwGoodSql = null;
        BufferedWriter bwBadSql = null;
        try {
            new File(GOOD_SQL_PATH).createNewFile();
            new File(BAD_SQL_PATH).createNewFile();

            bwGoodSql = new BufferedWriter(new FileWriter(GOOD_SQL_PATH));
            bwBadSql = new BufferedWriter(new FileWriter(BAD_SQL_PATH));

        } catch (IOException e1) {
            // TODO Auto-generated catch block
        }

        Class.forName(JDBC_DRIVER);

        Map<String, Connection> connMap = new HashMap<String, Connection>();
        Map<String, Statement> stmpMap = new HashMap<String, Statement>();

        int totalCnt = 0;
        int goodCnt = 0;
        int badCnt = 0;
        try (BufferedReader br = new BufferedReader(new FileReader("/home/james/_AllDocMap/hive_lineage_parser/20200723_success_all.sql"))) {
            String line = null;
            while (null != (line = br.readLine())) {
                System.out.println("=====>\n\t\t");
                System.out.println("totalCnt=" + totalCnt++);
                System.out.println("goodCnt=" + goodCnt + " -> good rate=" + Math.round(goodCnt * 100 / totalCnt) + "%");
                System.out.println("badCnt=" + badCnt);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                }
                String[] strArr = line.split("；");
                String db = strArr[0];
                String sql = strArr[1];

                String hiveServer2Url = DB_URL_PREFIX + db;
                Connection conn = null;
                Statement stmt = null;

                if (!connMap.containsKey(db)) {
//                    Class.forName(JDBC_DRIVER);
                    try {
                        conn = DriverManager.getConnection(hiveServer2Url, USER, PASS);
                        stmt = conn.createStatement();
                    } catch (Exception e) {
                        String exMsg = e.getMessage();
                        bwBadSql.write(line + "；        " + db + "；        " + exMsg);
                        bwBadSql.newLine();
                        bwBadSql.flush();
                        continue;
                    }
//                    stmt = conn.createStatement();
                    connMap.put(db, conn);
                    stmpMap.put(db,stmt);
                } else {
                    conn = connMap.get(db);
                    stmt = stmpMap.get(db);
//                    stmt = conn.createStatement();
                }


                /*
                 * execute hive sql
                 */
                try {
                    stmt.executeQuery(sql);
                    goodCnt++;
                    System.out.println("-------------------------------------------------------------");
                    System.out.println("goodCnt=" + goodCnt);
                    bwGoodSql.write(line);
                    bwGoodSql.newLine();
                    bwGoodSql.flush();
                } catch (Exception e) {
                    String exMsg = e.getMessage();
                    System.out.println(exMsg);
                    badCnt++;
                    bwBadSql.write(line + "；        " + db + "；        " + exMsg);
                    bwBadSql.newLine();
                    bwBadSql.flush();
                } // try
>>>>>>> ded0ccbf23b714c015759956328e4984bcbf7dae
            }
        } catch (IOException e) {
            String exMsg = e.getMessage();
        }
    }
<<<<<<< HEAD
}
=======
}
>>>>>>> ded0ccbf23b714c015759956328e4984bcbf7dae
