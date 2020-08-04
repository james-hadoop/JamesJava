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
 * 将DML插入Hive,生成HiveLineage,写入txkd_dc_hive_lineage_log
 * <p>
 * 输入：txkd_dc_hive_sql_with_table_names
 * 输出：txkd_dc_hive_lineage_log
 */
public class GenerateHiveLineageLogTool_4 {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        final String DB_URL = "jdbc:hive2://localhost:10000/txkd_dc_insert_db";
        final String USER = "";
        final String PASS = "";

        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        Statement stmt = conn.createStatement();

        int cnt = 0;
        List<ImmutablePair<String, String>> tableNameWithDdlList = HiveLineageMysqlUtil.readSqlWithTableNames(-1, -1);
        for (ImmutablePair<String, String> p : tableNameWithDdlList) {
            String sql = p.getLeft().replaceAll("TOK_SINGLE_QUOTE", "'").replaceAll("TOK_BACKSLASH_N", " ");
            if (sql.equals(Const.NULL)) {
                continue;
            }
            System.out.println("SQL:\n\t" + sql+"\n\n");
            HiveServerUtil.execDdl(sql);
            System.out.println(cnt++);
        } //for

        System.out.println(String.format("cnt=%s", cnt));

        System.exit(0);
    }
}
