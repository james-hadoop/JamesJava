package com.james.hive.lineage.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:12
 */
public class MysqlSqlWriter implements ISqlWriter1Tuple {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    private String dbUrl;
    private String userName;
    private String passwd;
    private String table;

    public MysqlSqlWriter() {
    }

    public MysqlSqlWriter(String dbUrl, String userName, String passwd, String table) {
        this.dbUrl = dbUrl;
        this.userName = userName;
        this.passwd = passwd;
        this.table = table;
    }

    public MysqlSqlWriter(String table) {
        new MysqlSqlWriter("jdbc:mysql://localhost:3306/developer", "developer", "developer", table);
    }

    @Override
    public int writeSql(Set<String> sqlSet) {
        int writtenCnt = 0;

        try {
            String formatDate = format(new Date());

            Class.forName(JDBC_DRIVER);

            Connection conn = DriverManager.getConnection(dbUrl, userName, passwd);

            String sql = "INSERT INTO " + table + " (lineage_str,update_time) VALUES (?,?)";
            PreparedStatement psmt = conn.prepareStatement(sql);

            for (String s : sqlSet) {
                psmt.setString(1, s);
                psmt.setString(2, formatDate);
                psmt.addBatch();

                writtenCnt++;
            }

            psmt.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writtenCnt;
    }

    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static Date parse(String dateStr) throws ParseException {
        return threadLocal.get().parse(dateStr);
    }

    public static String format(Date date) {
        return threadLocal.get().format(date);
    }
}
