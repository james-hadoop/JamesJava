package com.james.hive.lineage.tools;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class RunSqlOnHiveMultiThreadTool {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
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
                    stmpMap.put(db, stmt);
                } else {
                    conn = connMap.get(db);
                    stmt = stmpMap.get(db);
//                    stmt = conn.createStatement();
                }


                /*
                 * execute hive sql
                 */
                try {
                    ResultSet rs = stmt.executeQuery(sql);
                    int rsSize = rs.getFetchSize();
                    System.out.println("rsSize=" + rsSize);
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
            }
        } catch (IOException e) {
            String exMsg = e.getMessage();
        }
    }
}