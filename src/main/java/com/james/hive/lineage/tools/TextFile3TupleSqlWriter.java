package com.james.hive.lineage.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Set;

public class TextFile3TupleSqlWriter implements ISqlWriter3Tuple {
    private String sqlFileDir;
    private String sqlFilePrefix;
    private String sqlSuccessPureFilePath;
    private String sqlSuccessFilePath;
    private String sqlFailFilePath;
    private String sqlDdlFilePath;

    public TextFile3TupleSqlWriter(String sqlFileDir, String sqlFilePrefix) {
        this.sqlFileDir = sqlFileDir;
        this.sqlFilePrefix = sqlFilePrefix;
        this.sqlSuccessPureFilePath = sqlFileDir + sqlFilePrefix + "_success_pure.sql";
        this.sqlSuccessFilePath = sqlFileDir + sqlFilePrefix + "_success.sql";
        this.sqlFailFilePath = sqlFileDir + sqlFilePrefix + "_fail.sql";
        this.sqlDdlFilePath = sqlFileDir + sqlFilePrefix + "_hive_tables.sql";
    }

    @Override
    public int writeSql(Set<String> successSqlSet, Set<String> failSqlSet, Set<String> dbTableSet) {
        try {
            new File(sqlSuccessFilePath).createNewFile();
            new File(sqlFailFilePath).createNewFile();
            new File(sqlDdlFilePath).createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlSuccessFilePath))) {
            for (String s : successSqlSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlFailFilePath))) {
            for (String s : failSqlSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlDdlFilePath))) {
            for (String s : dbTableSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    public int writeSql(Set<String> successPureSqlSet, Set<String> successSqlSet, Set<String> failSqlSet, Set<String> dbTableSet) {
        try {
            new File(sqlSuccessPureFilePath).createNewFile();
            new File(sqlSuccessFilePath).createNewFile();
            new File(sqlFailFilePath).createNewFile();
            new File(sqlDdlFilePath).createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlSuccessPureFilePath))) {
            for (String s : successPureSqlSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlSuccessFilePath))) {
            for (String s : successSqlSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlFailFilePath))) {
            for (String s : failSqlSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlDdlFilePath))) {
            for (String s : dbTableSet) {
                bw.write(s);
                bw.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public void writeSql() {
        // TODO
    }
}
