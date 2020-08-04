package com.james.hive.lineage.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by James on 20-7-23 下午10:15
 */
public class TextFileSqlReader implements ISqlReader {
    private String sqlFilePath;

    public TextFileSqlReader() {
    }

    public TextFileSqlReader(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    @Override
    public Set<String> readSql() {
        Set<String> sqlSet = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(sqlFilePath))) {
            String line = null;
            while (null != (line = br.readLine())) {
                sqlSet.add(line);
            }
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sqlSet;
    }
}