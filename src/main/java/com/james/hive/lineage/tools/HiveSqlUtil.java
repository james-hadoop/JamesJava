package com.james.hive.lineage.tools;

public class HiveSqlUtil {
    public static final String DIVIDER = "；";
    public static final String SPACE8 = "        ";

    public static String mysql2hiveSql(String sql) {
        if (null == sql || sql.isEmpty()) {
            return null;
        }

        return sql.replaceAll("TOK_SINGLE_QUOTE", "'").replaceAll("TOK_TAB", " ").replaceAll("TOK_BACKSLASH_N", " ").replaceAll("TOK_BACKSLASH_R", " ");
    }
}
