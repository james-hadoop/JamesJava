package com.james.hive.lineage.tct.tool;

import com.james.hive.lineage.tct.common.Const;
import com.james.hive.lineage.tct.utils.HiveLineageMysqlUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * create by James on 2020-06-29.
 * <p>
 * 获取Hive Table的名称
 * 通过TDW接口获取字段信息，拼接DDL
 * <p>
 * 输入：txkd_dc_h_sql_with_table_names
 * 输出：txkd_dc_hive_sql_table_names_with_ddl
 */
public class GenerateTdwTableDdlTool_2 {
    public static void main(String[] args) throws SQLException {
        Set<String> tableNameSet = new HashSet<String>();
        Date date = new Date();

        List<ImmutablePair<String, String>> sqlWithDdlList = HiveLineageMysqlUtil.readSqlWithTableNames(-1,-1);
        for (ImmutablePair<String, String> p : sqlWithDdlList) {
            String tableNames = p.getRight();
            System.out.println(String.format(">>> tableNames=%s",tableNames));

            if (tableNames.equals(Const.NULL)) {
//                HiveLineageMysqlUtil.writeSqlTableNamesWithDdl(tableNames, Const.NULL, date);
                continue;
            }

            String[] tableNameArr = tableNames.split("&&&");
            for (int i = 0; i < tableNameArr.length; i++) {
                System.out.println(tableNameArr[i]);
                tableNameSet.add(tableNameArr[i]);
            }
            System.out.println("-----------------------------------------------------------------------------");
        } // for

        System.out.println(">>> DDLs below: ------------------------------------");
        List<String> ddlList = TdwSqlUtil.tryMakeTdwTableDdls(tableNameSet);
        System.out.println("------------------------------------------------------------------");
        for (String s : ddlList) {
            System.out.println(s);
            HiveLineageMysqlUtil.writeSqlTableNamesWithDdl(Const.NULL, s, date);
//            HiveServerUtil.execDdl(s);
        }
    }
}
