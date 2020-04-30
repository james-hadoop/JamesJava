package com.james._demo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class QuoteAdder {

    public static void main(String[] args) throws IOException {
        int ret = addQuote("data/rowkey/src.txt", "data/rowkey/des.txt");
        System.out.println("The result is: " + ret);
    }

    private static int addQuote(String srcPath, String desPath) throws IOException {
        if (null == srcPath || 0 == srcPath.length() || null == desPath || 0 == desPath.length()) {
            return -1;
        }

        BufferedReader br = new BufferedReader(new FileReader(srcPath));

        List<String> list = new ArrayList<String>();
        String line = null;
        while (null != (line = br.readLine())) {
            list.add("'" + line + "',");
        }

        File file = new File(desPath);
        file.createNewFile();

        BufferedWriter bw = new BufferedWriter(new FileWriter(desPath));
        for (String str : list) {
            bw.write(str);
            bw.newLine();
        }

        bw.close();
        br.close();

        return 0;
    }
}
