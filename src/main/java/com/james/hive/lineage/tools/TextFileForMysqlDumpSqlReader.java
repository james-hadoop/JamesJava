package com.james.hive.lineage.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TextFileForMysqlDumpSqlReader implements ISqlReader {
    private String sqlFilePath;

    public TextFileForMysqlDumpSqlReader(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    @Override
    public Set<String> readSql() {
        Set<String> sqlSet = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(sqlFilePath))) {
            String line = null;
            while (null != (line = br.readLine())) {
                sqlSet.add(line);
//                String[] strArr = line.split("\t");
//
//                if (strArr.length == 4) {
//                    sqlSet.add(strArr[3]);
//                }
            }
            return sqlSet;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sqlSet;
    }
}