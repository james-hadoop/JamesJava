package com.james.hive.lineage;

import java.sql.*;

public class HiveLineageAccess {
    private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/sng_mediaaccount_app", "", "");

        Statement stmt = conn.createStatement();
        String sql;
        ResultSet rs;

//        String tableName = "t_dwt_consume_video_rowkeyperformance_normal_d";
//
//        // show tables
//        sql = "show tables '" + tableName + "'";
//        System.out.println("Running:\n\t" + sql);
//        rs = stmt.executeQuery(sql);
//        if (rs.next()) {
//            System.out.println(rs.getString(1));
//        }
//        printDivider();
//
//        // describe table
//        sql = "describe " + tableName;
//        System.out.println("Running:\n\t" + sql);
//        rs = stmt.executeQuery(sql);
//        while (rs.next()) {
//            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
//        }
//        printDivider();
//
//        // select * query
//        sql = "select * from " + tableName;
//        System.out.println("Running:\n\t" + sql);
//        rs = stmt.executeQuery(sql);
//        while (rs.next()) {
//            System.out.println(rs.getString("name") + "\t" + rs.getInt("age") + "\t" + rs.getFloat("gpa"));
//        }
//        printDivider();

        // insert into * query
        sql = "";
        System.out.println("Running:\n\t" + sql);

        try {
            rs = stmt.executeQuery(sql);

            if (null == rs) {
                System.out.println("null == rs");
            } else {
                System.out.println("rs is not null");
            }
        } catch (Exception e) {
            // e.printStackTrace();
            String ex_msg = e.getMessage();
            // System.out.println(ex_msg);

            if (ex_msg.equals("The query did not generate a result set!")) {
                // SQL: insert into ... select ... 执行完毕
                System.out.println("SQL: insert into ... select ... 执行完毕");
                // TODO SQL成功执行之后，MySQL会多一条hive_lineage_log的记录，接下来需要解析这条记录，转成hive_linage_rel记录
            } else {
                // 其它类型的Exception
                // DO Nothing
            }
        }
    }
}