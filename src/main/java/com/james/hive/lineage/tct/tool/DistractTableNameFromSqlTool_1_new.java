package com.james.hive.lineage.tct.tool;

import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * create by James on 2020-06-29.
 * <p>
 * 将TDW SQL转化为Hive SQL
 * 从Hive SQL中解析出涉及到的所有Hive表的表名
 * <p>
 * 输入：txkd_dc_hive_sql_focus
 * 输出：txkd_dc_hive_sql_with_table_names
 */
public class DistractTableNameFromSqlTool_1_new {
    private static final Logger LOG = LoggerFactory.getLogger(DistractTableNameFromSqlTool_1_new.class);

    public static void main(String[] args) {
        String sql_str = Const.NULL;
        String tableNames = Const.NULL;

        List<String> sqlList = null;
        try {
            sqlList = HiveLineageMysqlUtil.readSqlFocus(-1);
        } catch (Exception ex) {
            System.out.println(">>> "+ex.getMessage());
        }
        for (String s : sqlList) {
            String dml = TdwSqlUtil.sqlTF(s.replaceAll("HEIHEIHEIAABBCC", " "));

            if (null != dml) {
                sql_str = dml.replaceAll("TOK_SINGLE_QUOTE", "'").replaceAll("TOK_TAB", " ").replaceAll("TOK_BACKSLASH_N", " ");
            }

            System.out.println("SQL:\n\t" + sql_str);
            HiveSqlParser parser = new HiveSqlParser(sql_str);
//            System.out.println("--------------------------------------------------------------------------");
            Set<String> tdwTables = parser.distrctTablesFromSql();
            if (null != tdwTables) {
                StringBuilder sb = new StringBuilder();
                for (String t : tdwTables) {
                    sb.append(t + "&&&");
                }
                tableNames = sb.toString();
            }

            try {
                HiveLineageMysqlUtil.writeSqlWithTableNames(dml, tableNames, new Date());
            } catch (Exception ex) {
                System.out.println(">>> "+ex.getMessage());
                System.out.println("--------------------------------------------------------------------------");
                System.out.println(sql_str);
            }
        }
    }
}
