package com.james.hive.lineage.tools;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by James on 20-7-24 上午12:01
 */
public class SqlProcessorDriver2 {
    public static void main(String[] args) {
        ISqlReader mysqlSqlReader = new TextFileForMysqlDumpSqlReader("F:\\20200724.txt");
//        ISqlReader mysqlSqlReader = new MysqlSqlReader("hive_lineage_log");

        ISqlWriter1Tuple textFileSqlWriter = new TextFileSqlWriter("20200724_out.sql");
        ISqlWriter1Tuple textFileSqlWriter2 = new TextFileSqlWriter("20200724_out_2.sql");

//        /*
//         * sqlProcessor1
//         */
//        AbstractSqlProcessor sqlProcessor1 = new ConcreteSqlProcessor(mysqlSqlReader, textFileSqlWriter);
//        sqlProcessor1.doWorkOnSql();

        /*
         * sqlProcessor2
         */
        AbstractSqlProcessor sqlProcessor2 = new AbstractSqlProcessor(mysqlSqlReader, textFileSqlWriter2) {
            @Override
            public Set<String> processSql(Set<String> sqlSet) {
                Set<String> processedSqlSet = new HashSet<>();

                for (String s : sqlSet) {
                    processedSqlSet.add("\tsqlProcessor2 => " + s);
                }
                return processedSqlSet;
            }
        };

        sqlProcessor2.doWorkOnSql();
    }
}
