package com.james.hive.lineage.tools;

import com.james.hive.lineage.tct.HiveSqlParser;

import java.util.HashSet;
import java.util.Set;

public class DistractHiveTableFromPythonedSqlTool {
    public static void main(String[] args) {
        final String FILE_DIR = "F:\\";
        final String SQL_FILE_PREFIX = "20200723";
        final String SQL_FILE_PATH = FILE_DIR + SQL_FILE_PREFIX + ".txt";

        ISqlReader mysqlSqlReader = new TextFileForMysqlDumpSqlReader(SQL_FILE_PATH);
        TextFile3TupleSqlWriter textFileSqlWriter = new TextFile3TupleSqlWriter(FILE_DIR, SQL_FILE_PREFIX);

        Set<String> sqlSet = mysqlSqlReader.readSql();
        if (null == sqlSet || sqlSet.isEmpty()) {
            return;
        }

        Set<String> successSqlSet = new HashSet<>();
        Set<String> successPureSqlSet = new HashSet<>();
        Set<String> failSqlSet = new HashSet<>();
        Set<String> dbTableSet = new HashSet<>();
        for (String s : sqlSet) {
            System.out.println(s);
            System.out.println("----------------------------------------------------------");
            String[] sArr = HiveSqlUtil.mysql2hiveSql(s).split("\t");
            System.out.println("size=" + sArr.length);

            if (sArr.length < 4) {
                continue;
            }

            String username = sArr[0];
            String db = sArr[1];
            String sha = sArr[2];
            String sql = sArr[3];

            String hiveSql = HiveSqlUtil.mysql2hiveSql(sql);
            HiveSqlParser hiveSqlParser = new HiveSqlParser(hiveSql, db);

            try {
                Set<String> tableSet = hiveSqlParser.distrctTablesFromSql();
                dbTableSet.addAll(tableSet);
                successSqlSet.add(db + HiveSqlUtil.DIVIDER + hiveSql + ";");
                successPureSqlSet.add(hiveSql + ";");
            } catch (Exception e) {
                failSqlSet.add(sha + HiveSqlUtil.DIVIDER + HiveSqlUtil.SPACE8 + hiveSql + ";");
            }
        }

        textFileSqlWriter.writeSql(successPureSqlSet, successSqlSet, failSqlSet, dbTableSet);
    }
}