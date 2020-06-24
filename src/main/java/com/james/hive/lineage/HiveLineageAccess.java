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
        sql = "INSERT INTO t_dwt_consume_video_rowkeyperformance_normal_d SELECT 20200607 AS ftime, 'kd' AS sp_app_id, rowkey AS rowkey, NULL AS video_type, baoguang_cnt AS maintl_exp_cnt , main_click AS maintl_click_cnt, onetothree_expose AS floatlayer_exp_cnt, onetothree_vv AS floatlayer_vv, videochn_expose AS videochann_exp_cnt, videochn_click AS videochann_click_cnt , kddaily_expose AS infkddaily_exp_cnt, kddaily_click AS infkddaily_click_cnt, video_vv AS total_vv, valid_vv AS valid_vv, dianzan_cnt AS like_cnt , share_cnt AS share_cnt, biu_cnt AS biu_cnt, main_comment AS mainclass_cmt_cnt, main_comment_zan AS mainclass_cmtlike_cnt, sub_comment AS sub2class_cmt_cnt , sub_comment_zan AS sub2class_like_cnt, NULL AS collect_cnt, NULL AS accuse_cnt, NULL AS negfeeback_cnt, CAST(FLOOR(tot_watch_duration) AS BIGINT) AS duration_video FROM sng_mediaaccount_app.kandian_video_medium_full_info_d WHERE ftime = 20200607";
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