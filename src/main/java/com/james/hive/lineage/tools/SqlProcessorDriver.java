package com.james.hive.lineage.tools;

import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:28
 */
public class SqlProcessorDriver {
    public static void main(String[] args) {
        ISqlReader textFileSqlReader = new TextFileSqlReader("20200724.txt");
        ISqlReader mysqlSqlReader = new MysqlSqlReader("hive_lineage_log");

        ISqlWriter1Tuple textFileSqlWriter = new TextFileSqlWriter("20200724_out.sql");

        Set<String> sqlSet = mysqlSqlReader.readSql();
        int cnt = textFileSqlWriter.writeSql(sqlSet);

        System.out.println(String.format("cnt=%s", cnt));
    }
}
