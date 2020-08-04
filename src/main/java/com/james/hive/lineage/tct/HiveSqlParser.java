package com.james.hive.lineage.tct;

import jline.internal.Log;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiveSqlParser {
    private String sql;
    private String db;

    private Set<String> tableList = new HashSet<>();

    private ParseDriver pd = new ParseDriver();

    public HiveSqlParser() {
    }

    public HiveSqlParser(String sql, String db) {
        this.sql = sql;
        this.db = db;
    }

    public HiveSqlParser(String sql) {
        this.sql = sql;
    }

    public Set<String> distrctTablesFromSql() throws Exception {
        if (this.sql == null || this.sql.isEmpty()) {
            return null;
        }

        ASTNode ast = null;
        try {
            ast = pd.parse(this.sql);
        } catch (Exception ex) {
//            Log.error(ex.getMessage());
            throw new Exception(ex);
        }
        if (null == ast) {
            return null;
        }

        parseIteral(ast);

        return this.tableList;
    }

    private void parseIteral(ASTNode ast) {
        parseChildNodes(ast);
        parseCurrentNode(ast);
    }

    private void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
//                case HiveParser.TOK_TABREF:// inputTable
//                    ASTNode tabTree = (ASTNode) ast.getChild(0);
//                    String tableName = (tabTree.getChildCount() == 1)
//                            ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
//                            : tabTree.getChild(1).toString();
//                    tableList.add(tableName);
//                    break;
                case HiveParser.TOK_TABNAME:
                    String tgtTable = (ast.getChildCount() == 1) ? db + "." + ast.getChild(0).getText() : ast.getChild(0).getText() + "." + ast.getChild(1).getText();
                    tableList.add(tgtTable);
                    break;

//                case HiveParser.TOK_INSERT_INTO:
//                    String tgtTable = ast.getChild(0).getChild(0).getChild(0).getText();
//                    tableList.add(tgtTable);
//                    break;
            }
        }
    }

    private void parseChildNodes(ASTNode ast) {
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                parseIteral(child);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String s3 = "";
        String sql = s3;

        HiveSqlParser parser = new HiveSqlParser(sql);

        Set<String> tdwTables = parser.distrctTablesFromSql();

        for (String s : tdwTables) {
            System.out.println(s);
        }
    }
}
