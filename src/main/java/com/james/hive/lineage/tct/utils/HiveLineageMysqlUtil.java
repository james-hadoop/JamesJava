package com.james.hive.lineage.tct.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HiveLineageMysqlUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageMysqlUtil.class);

    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    /**
     * 线程安全的日期格式化
     *
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static Date parse(String dateStr) throws ParseException {
        return threadLocal.get().parse(dateStr);
    }

    public static String format(Date date) {
        return threadLocal.get().format(date);
    }

    /**
     * 写txkd_dc_hive_sql_table_names_with_ddl
     *
     * @param table_names
     * @param table_names
     * @param date
     * @throws SQLException
     */
    public static void writeSqlTableNamesWithDdl(String table_names, String ddl, Date date) throws SQLException {
        if (null == table_names || table_names.isEmpty()) {
            return;
        }

        LOG.info(String.format("%s", table_names));

        Connection conn = HiveLineageMysqlConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get Mysql connection.");
        }

        String sql = "INSERT INTO txkd_dc_hive_sql_table_names_with_ddl(table_names, ddl, update_time) VALUES (?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, table_names);
        ps.setString(2, ddl);
        String updateTime = format(date);
        ps.setString(3, updateTime);

        int resultSet = ps.executeUpdate();
        if (resultSet > 0) {
            //如果插入成功，则打印success
            LOG.info("writeSqlTableNamesWithDdl() success...");
        } else {
            //如果插入失败，则打印Failure
            LOG.info("writeSqlTableNamesWithDdl() fail...");
        }
    }

    /**
     * 读txkd_dc_hive_sql_table_names_with_ddl
     *
     * @param cnt
     * @return
     * @throws SQLException
     */
    public static List<ImmutablePair<String, String>> readSqlTableNamesWithDdl(int cnt) throws SQLException {
        Connection conn = HiveLineageMysqlConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get Mysql connection.");
        }

        Statement stmt = conn.createStatement();
        String sql = "SELECT table_names, ddl FROM txkd_dc_hive_sql_table_names_with_ddl";

        if (-1 != cnt) {
            sql = String.format("SELECT table_names, ddl FROM txkd_dc_hive_sql_table_names_with_ddl limit %s", cnt);
        }

        ResultSet rs = stmt.executeQuery(sql);

        List<ImmutablePair<String, String>> sqlList = new ArrayList<>();
        while (rs.next()) {
            String sqlStr = rs.getString(1);
            String ddl = rs.getString(2);
            sqlList.add(new ImmutablePair<String, String>(sqlStr, ddl));
        }

        return sqlList;
    }

    /**
     * 写txkd_dc_hive_sql_with_table_names
     *
     * @param sql_str
     * @param table_names
     * @param date
     * @throws SQLException
     */
    public static void writeSqlWithTableNames(String sql_str, String table_names, Date date) throws SQLException {
        if (null == sql_str || sql_str.isEmpty()) {
            return;
        }

        LOG.info(String.format("%s", sql_str));

        Connection conn = HiveLineageMysqlConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get Mysql connection.");
        }

        String sql = "INSERT INTO txkd_dc_hive_sql_with_table_names(sql_str, table_names, update_time) VALUES (?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, sql_str);
        ps.setString(2, table_names);
        String updateTime = format(date);
        ps.setString(3, updateTime);

        int resultSet = ps.executeUpdate();
        if (resultSet > 0) {
            //如果插入成功，则打印success
            LOG.info("insertSqlWithDdl() success...");
        } else {
            //如果插入失败，则打印Failure
            LOG.info("insertSqlWithDdl() fail...");
        }
    }

    /**
     * 读txkd_dc_hive_sql_with_table_names
     *
     * @param cnt
     * @return
     * @throws SQLException
     */
    public static List<ImmutablePair<String, String>> readSqlWithTableNames(int cnt, int id) throws SQLException {
        Connection conn = HiveLineageMysqlConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get Mysql connection.");
        }

        Statement stmt = conn.createStatement();
        String sql = "SELECT sql_str, table_names FROM txkd_dc_hive_sql_with_table_names";

        if (-1 != cnt ) {
            sql = String.format("SELECT sql_str, table_names FROM txkd_dc_hive_sql_with_table_names limit %s", cnt);
        }

        if (-1 != id ) {
            sql = String.format("SELECT sql_str, table_names FROM txkd_dc_hive_sql_with_table_names where id=%s limit %s", id, cnt);
        }

        ResultSet rs = stmt.executeQuery(sql);

        List<ImmutablePair<String, String>> sqlList = new ArrayList<>();
        while (rs.next()) {
            String sqlStr = rs.getString(1);
            String tableNames = rs.getString(2);
            sqlList.add(new ImmutablePair<String, String>(sqlStr, tableNames));
        }

        return sqlList;
    }

    /**
     * 读txkd_dc_hive_sql_focus_clean
     *
     * @param cnt
     * @return
     * @throws SQLException
     */
    public static List<String> readSqlFocus(int cnt) throws SQLException {
        Connection conn = HiveLineageMysqlConn.getInstance().getConn();
        if (null == conn) {
            LOG.error("Failed to get Mysql connection.");
        }

        Statement stmt = conn.createStatement();
        String sql = "SELECT sql_str FROM txkd_dc_hive_sql_focus";

        if (-1 != cnt) {
            sql = String.format("SELECT sql_str FROM txkd_dc_hive_sql_focus order by sql_str limit %s", cnt);
        }

        ResultSet rs = stmt.executeQuery(sql);

        List<String> sqlList = new ArrayList<>();
        while (rs.next()) {
            String sqlStr = rs.getString(1);
            sqlList.add(sqlStr);
        }

        return sqlList;
    }

    public static void main(String[] args) throws SQLException {
//        String sql_str = "sql_str";
//        String ddl = "ddl";
//        Date date = new Date();
//
//        writeSqlWithDdl(sql_str, ddl, date);

        List<ImmutablePair<String, String>> sqlWithDdlList = readSqlWithTableNames(3,-1);
        for (ImmutablePair<String, String> p : sqlWithDdlList) {
            System.out.println(p.getLeft() + " -> " + p.getRight());
        }

//        List<String> sqlList = readSqlFocus(3);
//        for (String s : sqlList) {
//            System.out.println(s);
//        }


    }
}