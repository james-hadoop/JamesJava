package com.james.hive.lineage.tct;

import com.james.utils.JamesUtil;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiveSqlUtil {
    /*
     * TableRelation
     */
    private static List<String> srcTables = new ArrayList<>();
    private static String tgtTable = new String();
    private static List<String> tableList = new ArrayList<>();

    private static ParseDriver pd = new ParseDriver();

    public static void parse(ASTNode ast) {
        parseIteral(ast);
    }

    private static void parseIteral(ASTNode ast) {
        parseChildNodes(ast);
        parseCurrentNode(ast);
    }

    private static void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_TABREF:// inputTable
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableName = (tabTree.getChildCount() == 1)
                            ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            : tabTree.getChild(1).toString();
                    srcTables.add(tableName);
                    tableList.add(tableName);
                    break;
                case HiveParser.TOK_INSERT_INTO:
                    tgtTable = ast.getChild(0).getChild(0).getChild(0).getText();
                    tableList.add(tgtTable);
                    break;
            }
        }
    }

    private static Set<String> parseChildNodes(ASTNode ast) {
        Set<String> set = new HashSet<String>();
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                parseIteral(child);
            }
        }
        return set;
    }

    public static void main(String[] args) {


        String sql = "";

        ASTNode ast = null;
        try {
            ast = pd.parse(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(null==ast){
            return;
        }
        System.out.println(ast.toStringTree());

        JamesUtil.printDivider();
        parse(ast);

        JamesUtil.printDivider();
//        System.out.println(tgtTable);
//        for (String t : srcTables) {
//            System.out.println(t);
//        }
        for (String t : tableList) {
            System.out.println(t);
        }
    }
}
