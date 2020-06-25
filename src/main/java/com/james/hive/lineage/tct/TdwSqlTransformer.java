package com.james.hive.lineage.tct;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * create by jamesqjiang on 2020-06-25.
 * TODO
 */
public class TdwSqlTransformer {
    public static Pair<List<String>, String> transform(String sql) {
        if (null == sql || sql.isEmpty()) {
            return null;
        }

        // 将insert...select..语句规范化成Hive SQL，该sql能够直接在Hive上执行
        String dml = TdwSqlUtil.sqlTF(sql);

        if (null == dml || dml.isEmpty()) {
            return null;
        }

        HiveSqlParser parser = new HiveSqlParser(dml);
        Set<String> tdwTables = parser.distrctTablesFromSql();
        System.out.println(String.format("tdwTables.size()=%s", tdwTables.size()));
        for (String s : tdwTables) {
            System.out.println("\t" + s);
        }

        // 用TDW接口获取数据表DDL
        List<String> ddlList = TdwSqlUtil.tryMakeTdwTableDdls(tdwTables);
        System.out.println(String.format("ddlList.size()=%s", ddlList.size()));
        for (String ddl : ddlList) {
            System.out.println(String.format("ddl=%s", ddl));
        }

        // 将ddl, dml塞入到二元组
        Pair<List<String>, String> sqlPair = new ImmutablePair<List<String>, String>(ddlList, dml);

        return sqlPair;
    }

    public static void main(String[] args) {
        System.out.println("TdwSqlTransformer begin...");


        String sql = "".replaceAll("SINGLE_QUOTE", "'");

        Pair<List<String>, String> sqlPair = TdwSqlTransformer.transform(sql);
        System.out.println(">>> ---------------------------------------------------------------");
        for (String ddl : sqlPair.getLeft()) {
            System.out.println(String.format("\tddl: \n%s", ddl));
        }

        System.out.println(String.format("\tdml: \n%s\n", sqlPair.getRight()));

        System.out.println("TdwSqlTransformer end...");
    }
}
