package com.james.hive.lineage.tct.tool;

import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;
import com.james.hive.lineage.tct.utils.HiveServerUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * create by James on 2020-06-29.
 * <p>
 * 连接Hive Server，执行DDL，创建Hive库表
 * <p>
 * 输入：txkd_dc_hive_sql_table_names_with_ddl
 * 输出：无
 */
public class CreateHiveTableTool_3 {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveTableTool_3.class);

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        final String DB_URL = "jdbc:hive2://localhost:10000/txkd_dc_insert_db";
        final String USER = "";
        final String PASS = "";

        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement stmt = conn.createStatement();

        int cnt = 0;
        List<ImmutablePair<String, String>> tableNameWithDdlList = HiveLineageMysqlUtil.readSqlTableNamesWithDdl(-1);
        for (ImmutablePair<String, String> p : tableNameWithDdlList) {
            String ddl = p.getRight();
            if (ddl.equals(Const.NULL)) {
                continue;
            }
            HiveServerUtil.execDdl(ddl);
            System.out.println(cnt++);
            Thread.sleep(500);

        } //for

        System.out.println(String.format("cnt=%s", cnt));

        System.exit(0);
    }
}
