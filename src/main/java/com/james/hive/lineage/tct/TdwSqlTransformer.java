package com.james.hive.lineage.tct;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * create by jamesqjiang on 2020-06-25.
 * TODO
 */
public class TdwSqlTransformer {
    public static Pair<List<String>, String> transform(String sql) throws Exception {
        if (null == sql || sql.isEmpty()) {
            return null;
        }

        // 将insert...select..语句规范化成Hive SQL，该sql能够直接在Hive上执行
        String dml = TdwSqlUtil.sqlTF(sql);

        if (null == dml || dml.isEmpty()) {
            return null;
        }

        HiveSqlParser parser = new HiveSqlParser(dml);
        Set<String> tdwTables = parser.distrctTablesFromSql();
        if (null == tdwTables || tdwTables.size() == 0) {
            return null;
        }
        System.out.println(String.format("tdwTables.size()=%s", tdwTables.size()));
        for (String s : tdwTables) {
            System.out.println("\t" + s);
        }

        // 用TDW接口获取数据表DDL
        List<String> ddlList = TdwSqlUtil.tryMakeTdwTableDdls(tdwTables);
        System.out.println(String.format("ddlList.size()=%s", ddlList.size()));
        for (String ddl : ddlList) {
            System.out.println(String.format("ddl=%s", ddl));
        }

        // 将ddl, dml塞入到二元组
        Pair<List<String>, String> sqlPair = new ImmutablePair<List<String>, String>(ddlList, dml);

        return sqlPair;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("TdwSqlTransformer begin...");

        String sql1 = "INSERT TABLE t_dwt_consume_video_rowkeyperformance_normal_d select 20200613 ftime, SINGLE_QUOTEkdSINGLE_QUOTE sp_app_id, rowkey rowkey, null video_type, baoguang_cnt maintl_exp_cnt, main_click maintl_click_cnt, onetothree_expose floatlayer_exp_cnt, onetothree_vv floatlayer_vv, videochn_expose videochann_exp_cnt, videochn_click videochann_click_cnt, kddaily_expose infkddaily_exp_cnt, kddaily_click infkddaily_click_cnt, video_vv total_vv, valid_vv valid_vv, dianzan_cnt like_cnt, share_cnt share_cnt, biu_cnt biu_cnt, main_comment mainclass_cmt_cnt, main_comment_zan mainclass_cmtlike_cnt, sub_comment sub2class_cmt_cnt, sub_comment_zan sub2class_like_cnt, null collect_cnt, null accuse_cnt, null negfeeback_cnt, CAST(floor(tot_watch_duration) AS BIGINT) AS duration_video from sng_mediaaccount_app::kandian_video_medium_full_info_d where ftime = 20200613";
        String sql2 = "INSERT OVERWRITE TABLE t_dwa_kd_video_hudong_ald_layer_1d_d PARTITION(ftime=20200613) SELECT 20200613 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE ,MOD(bigint1(conv(substr(md5(uin||SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) ,SUM(watch_puin_pv) ,SUM(watch_puin_pv_o2t) ,SUM(watch_puin_pv_ld) ,SUM(puin_page_pv) ,SUM(puin_page_pv_02t) ,SUM(puin_page_pv_ld) ,SUM(zan_pv) ,SUM(zan_pv_o2t) ,SUM(zan_pv_ld) ,SUM(d_zan_pv) ,SUM(d_zan_pv_o2t) ,SUM(d_zan_pv_ld) ,SUM(comment_pv) ,SUM(comment_pv_o2t) ,SUM(comment_pv_ld) ,SUM(biu_pv) ,SUM(biu_pv_o2t) ,SUM(biu_pv_ld) ,SUM(q_comment_pv) ,SUM(q_comment_pv_o2t) ,SUM(q_comment_pv_ld) ,SUM(comment_duration) FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEvideo_hudongSINGLE_QUOTE ) a RIGHT JOIN t_dwa_kd_video_hudong_ald_dwa_1d_d partition (p_20200613) b ON a.dt=b.ftime GROUP BY group_key ,MOD(bigint1(conv(substr(md5(uin||SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";

        String s1 = "INSERT OVERWRITE TABLE rikideng_DWD_small_scrip_session_talk PARTITION(ftime = 20200613) SELECT 20200613 AS ftime, op_type, if(op_type != SINGLE_QUOTE0X800A806SINGLE_QUOTE,d1,NULL) AS rowkey, d2 AS message_id, fromuin AS from_uin, touin AS to_uin FROM hlw::t_dw_dc01160 WHERE op_type IN (SINGLE_QUOTE0X800A805SINGLE_QUOTE, SINGLE_QUOTE0X800A806SINGLE_QUOTE, SINGLE_QUOTE0X800A80ESINGLE_QUOTE) AND tdbank_imp_date BETWEEN 2020061300 AND 2020061323";
        // bad
        String s2 = "INSERT OVERWRITE TABLE t_dwa_kd_video_hudong_ald_layer_1d_d PARTITION(ftime=20200613) SELECT 20200613 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) ,SUM(watch_puin_pv) ,SUM(watch_puin_pv_o2t) ,SUM(watch_puin_pv_ld) ,SUM(puin_page_pv) ,SUM(puin_page_pv_02t) ,SUM(puin_page_pv_ld) ,SUM(zan_pv) ,SUM(zan_pv_o2t) ,SUM(zan_pv_ld) ,SUM(d_zan_pv) ,SUM(d_zan_pv_o2t) ,SUM(d_zan_pv_ld) ,SUM(comment_pv) ,SUM(comment_pv_o2t) ,SUM(comment_pv_ld) ,SUM(biu_pv) ,SUM(biu_pv_o2t) ,SUM(biu_pv_ld) ,SUM(q_comment_pv) ,SUM(q_comment_pv_o2t) ,SUM(q_comment_pv_ld) ,SUM(comment_duration) FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEvideo_hudongSINGLE_QUOTE ) a RIGHT JOIN t_dwa_kd_video_hudong_ald_dwa_1d_d partition (p_20200613) b ON a.dt=b.ftime GROUP BY group_key ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";
        String s3 = "insert table kuaibao_ods_boss4715_log select 20200613 as imp_date, ftime, idfv, sos, shardware, logintype, openid, qq, phone_id, teamid, teamname, sidfv, binarymd5, copname, sappver, iisjb from omg_tdbank::mobile_t_boss_v1_4715 where tdbank_imp_date >= 2020061300 and tdbank_imp_date <= 2020061324\n";
        // bad
        String s4 = "INSERT OVERWRITE TABLE dwa_kd_daily_infite_ald_layer_1h_h PARTITION(ftime=2020061420) SELECT /*+ MAPJOIN(a)*/ 2020061420 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE as utype ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) as bucket_id ,SUM(daily_infite_tab_expose_pv) AS daily_unbounded_tab_exp_pv ,COUNT(IF(daily_infite_tab_expose_pv>0,uin,null)) AS daily_unbounded_tab_exp_uv ,SUM(daily_infite_tab_click_pv) AS daily_unbounded_tab_clk_pv ,COUNT(IF(daily_infite_tab_click_pv>0,uin,null)) AS daily_unbounded_tab_clk_uv ,SUM(daily_infite_expose_pv) AS daily_unbounded_exp_pv ,COUNT(IF(daily_infite_expose_pv>0,uin,null)) AS daily_unbounded_exp_uv ,SUM(daily_infite_click_pv) AS daily_unbounded_clk_pv ,COUNT(IF(daily_infite_click_pv>0,uin,null)) AS daily_unbounded_clk_uv ,COUNT(IF(daily_infite_dau3a>0,uin,null)) AS daily_unbounded_dau3a ,SUM(daily_infite_dur) AS daily_unbounded_duration ,COUNT(IF(daily_infite_dur>0,uin,null)) AS daily_unbounded_duration_uv ,SUM(daily_infite_pv) AS daily_unbounded_pic_pv ,COUNT(IF(daily_infite_pv>0,uin,null)) AS daily_unbounded_pic_uv ,SUM(daily_infite_vv) AS daily_unbounded_video_pv ,COUNT(IF(daily_infite_vv>0,uin,null)) AS daily_unbounded_video_uv FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEhour_reportSINGLE_QUOTE) a RIGHT JOIN dwa_kd_daily_infite_ald_dwa_1h_h partition (p_2020061420) b on a.dt=20200613 where b.ftime=2020061420 group by group_key,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";
        String s5 = "INSERT OVERWRITE TABLE dim_kb_newald_layer_vaild__info PARTITION(ftime=20200613) select 20200613,layer_id,experiment_id from dim_kb_newald_layer_start_exp_info where ftime=20200613 and crt_time < 20200613 and exp_dt <20200613 and exp_dt is not null\n";

        String sql = sql2.replaceAll("SINGLE_QUOTE", "'");
        System.out.println(sql);
        System.out.println("-----------------------------------------------------------------------------------------");

        Pair<List<String>, String> sqlPair = TdwSqlTransformer.transform(sql);
        System.out.println(">>> ---------------------------------------------------------------");
        if (null == sqlPair) {
            return;
        }

        for (String ddl : sqlPair.getLeft()) {
            System.out.println(String.format("\tddl: \n%s", ddl));
        }

        System.out.println(String.format("\tdml: \n%s\n", sqlPair.getRight()));

        System.out.println("TdwSqlTransformer end...");
    }
}
