package com.james.hive.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql转换工具
 *
 * @author ericxun(荀皓)
 */

public class SQLTransForm {

    private static String[] pattern;
    private static String[] replace;

    /**
     * 初始化，用数组来存储被替换和替换的字符串
     */
    private static void init() {

        pattern = new String[8];
        replace = new String[8];

        pattern[0] = "(?i)INSERT\\s+TABLE";
        replace[0] = "INSERT INTO";

        pattern[1] = "(?i)INSERT\\s+OVERWRITE\\s+TABLE";
        replace[1] = "INSERT INTO";

        pattern[2] = "::";
        replace[2] = ".";

        pattern[3] = "\\\\t";
        replace[3] = "BACKSLASH_T";

        pattern[4] = "\\\\r";
        replace[4] = "BACKSLASH_R";

        pattern[5] = "\\\\n";
        replace[5] = "BACKSLASH_N";

        pattern[6] = "(?i)partition\\(\\s*.+?\\s*\\)";
        replace[6] = " ";

        pattern[7] = "distribute\\s+by\\s+.+";
        replace[7] = ";";


    }

    /**
     * unbase64(列名)  ->  cast(unbase64(列名) as string
     *
     * @param str 传入要被转换字符串
     */
    public static String unbase64TF(String str) {
        String pattern = "(?i)unbase64\\(\\s*.+?\\s*\\)"; // 加上？可以最大匹配，不加是最小匹配
        Pattern compile = Pattern.compile(pattern);
        Matcher matcher = compile.matcher(str);
        while (matcher.find()) {
            String group = matcher.group();
            String s1 = group.replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)");
            str = str.replaceAll(s1, "cast(" + group + " as string)");
        }
        return str;
    }


    /**
     * 主方法
     *
     * @param str 传入要被转换字符串
     */
    public static String sqlTF(String str) {
        init();
        for (int i = 0; i < pattern.length; i++) {
            str = str.replaceAll(pattern[i], replace[i]);
        }
        return unbase64TF(str);
    }

    public static void main(String[] args) {}
}