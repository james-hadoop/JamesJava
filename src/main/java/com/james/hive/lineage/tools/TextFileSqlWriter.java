package com.james.hive.lineage.tools;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:01
 */
public class TextFileSqlWriter implements ISqlWriter1Tuple {
    private String sqlFilePath;

    public TextFileSqlWriter() {
    }

    public TextFileSqlWriter(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    @Override
    public int writeSql(Set<String> sqlSet) {
        int writtenCnt = 0;

        try {
            new File(sqlFilePath).createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(sqlFilePath))) {
            for (String s : sqlSet) {
                bw.write(s);
                bw.newLine();
                writtenCnt++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writtenCnt;
    }
}
