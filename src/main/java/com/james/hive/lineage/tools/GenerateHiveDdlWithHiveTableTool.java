package com.james.hive.lineage.tools;

import com.james.hive.lineage.tct.TdwSqlUtil;

import java.io.*;

public class GenerateHiveDdlWithHiveTableTool {
    public static void main(String[] args) throws IOException {
        String tableInfoPath = "F:\\20200723_hive_tables.sql";
        String ddlPath = "F:\\20200723_ddl.sql";
        String failSqlPath = "F:\\20200723_ddl_fail.sql";
        File file2 = new File(failSqlPath);
        file2.createNewFile();
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(failSqlPath));

        File file = new File(ddlPath);
        file.createNewFile();

        BufferedReader br = new BufferedReader(new FileReader(tableInfoPath));
        BufferedWriter bw = new BufferedWriter(new FileWriter(ddlPath));

        String line = null;
        while (null != (line = br.readLine())) {
            System.out.println("\n\n---------------------------------------------------------");
            System.out.println(String.format("line=\n\t%s", line));

            String[] strArr = line.split("\\.");
//            System.out.println(strArr.length);
            if (strArr.length < 2) {
                continue;
            }
            String db = strArr[0];
            String table = strArr[1];

            String tdwTableDDL = null;

            try {
                tdwTableDDL = TdwSqlUtil.makeTdwTableDdl(db, table);
                bw.write(tdwTableDDL);
                bw.newLine();
                bw.newLine();
            } catch (Exception e) {
                // do nothing
                bw2.write(line);
                bw2.newLine();
            }
        } //while

        bw2.close();
        bw.close();
    }
}
