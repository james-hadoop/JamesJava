package com.james.hive.lineage.tct.tool;

import com.james.hive.lineage.tct.TdwSqlUtil;
import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * create by jamesqjiang on 2020-06-29.
 * <p>
 * 将TDW SQL转化为Hive SQL
 * 从Hive SQL中解析出涉及到的所有Hive表的表名
 * <p>
 * 输入：txkd_dc_hive_sql_focus_clean
 * 输出：txkd_dc_h_sql_with_table_names
 */
public class DistractTableNameFromSqlTool_1 {
    public static void main(String[] args) throws SQLException {
        String sql_str = Const.NULL;
        String tableNames = Const.NULL;

        List<String> sqlList = HiveLineageMysqlUtil.readSqlFocus(-1);
        for (String s : sqlList) {
            String dml = TdwSqlUtil.sqlTF(s);

            if (null != dml) {
                sql_str = dml.replaceAll("SINGLE_QUOTE", "'");
            }

            System.out.println("SQL:\n\t"+sql_str);
            HiveSqlParser parser = new HiveSqlParser(sql_str);
            System.out.println("--------------------------------------------------------------------------");
            Set<String> tdwTables = parser.distrctTablesFromSql();
            if (null != tdwTables) {
                StringBuilder sb = new StringBuilder();
                for (String t : tdwTables) {
                    sb.append(t + "&&&");
                }
                tableNames = sb.toString();
            }

            HiveLineageMysqlUtil.writeSqlWithTableNames(sql_str, tableNames, new Date());
        }


    }
}
