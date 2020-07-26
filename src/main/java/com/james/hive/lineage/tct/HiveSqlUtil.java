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
        String s1 = "INSERT INTO rikideng_DWD_small_scrip_session_talk SELECT 20200613 AS ftime, op_type, if(op_type != SINGLE_QUOTE0X800A806SINGLE_QUOTE,d1,NULL) AS rowkey, d2 AS message_id, fromuin AS from_uin, touin AS to_uin FROM hlw.t_dw_dc01160 WHERE op_type IN (SINGLE_QUOTE0X800A805SINGLE_QUOTE, SINGLE_QUOTE0X800A806SINGLE_QUOTE, SINGLE_QUOTE0X800A80ESINGLE_QUOTE) AND tdbank_imp_date BETWEEN 2020061300 AND 2020061323";

        String s2 = "INSERT INTO t_dwa_kd_video_hudong_ald_layer_1d_d SELECT 20200613 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) ,SUM(watch_puin_pv) ,SUM(watch_puin_pv_o2t) ,SUM(watch_puin_pv_ld) ,SUM(puin_page_pv) ,SUM(puin_page_pv_02t) ,SUM(puin_page_pv_ld) ,SUM(zan_pv) ,SUM(zan_pv_o2t) ,SUM(zan_pv_ld) ,SUM(d_zan_pv) ,SUM(d_zan_pv_o2t) ,SUM(d_zan_pv_ld) ,SUM(comment_pv) ,SUM(comment_pv_o2t) ,SUM(comment_pv_ld) ,SUM(biu_pv) ,SUM(biu_pv_o2t) ,SUM(biu_pv_ld) ,SUM(q_comment_pv) ,SUM(q_comment_pv_o2t) ,SUM(q_comment_pv_ld) ,SUM(comment_duration) FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEvideo_hudongSINGLE_QUOTE ) a RIGHT JOIN t_dwa_kd_video_hudong_ald_dwa_1d_d partition (p_20200613) b ON a.dt=b.ftime GROUP BY group_key ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";

        String s3 = "INSERT INTO kuaibao_ods_boss4715_log select 20200613 as imp_date, ftime, idfv, sos, shardware, logintype, openid, qq, phone_id, teamid, teamname, sidfv, binarymd5, copname, sappver, iisjb from omg_tdbank.mobile_t_boss_v1_4715 where tdbank_imp_date >= 2020061300 and tdbank_imp_date <= 2020061324";


        String sql = s3;

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
