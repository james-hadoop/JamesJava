package com.james.hive.lineage.tools;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by James on 20-7-23 下午10:22
 */
public class MysqlSqlReader implements ISqlReader {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    private String dbUrl;
    private String userName;
    private String passwd;
    private String table;

    public MysqlSqlReader() {
    }

    public MysqlSqlReader(String dbUrl, String userName, String passwd, String table) {
        this.dbUrl = dbUrl;
        this.userName = userName;
        this.passwd = passwd;
        this.table = table;
    }

    public MysqlSqlReader(String table) {
        this("jdbc:mysql://localhost:3306/developer", "developer", "developer", table);
    }

    @Override
    public Set<String> readSql() {
        Set<String> sqlSet = new HashSet<>();

        try {
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(dbUrl, userName, passwd);
            Statement stmt = conn.createStatement();

            String sql = "SELECT lineage_str FROM " + table;

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String sqlStr = rs.getString(1);
                sqlSet.add(sqlStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sqlSet;
    }

    public static void main(String[] args) {
        String dbUrl = "jdbc:mysql://localhost:3306/developer";
        String userName = "developer";
        String passwd = "developer";
        String table = "hive_lineage_log";

        ISqlReader sqlGetter = new MysqlSqlReader(table);
        Set<String> sqlSet = sqlGetter.readSql();

        for (String s : sqlSet) {
            System.out.println(s);
        }
    }
}
