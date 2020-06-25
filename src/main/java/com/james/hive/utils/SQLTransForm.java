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

    public static void main(String[] args) {
        String str1 = "INSERT TABLE t_dwt_consume_video_rowkeyperformance_normal_d\n" +
                "SELECT 20200607 ftime,\n" +
                "       'kd' sp_app_id,\n" +
                "            rowkey rowkey,\n" +
                "            NULL video_type,\n" +
                "                 baoguang_cnt maintl_exp_cnt,\n" +
                "                 main_click maintl_click_cnt,\n" +
                "                 onetothree_expose floatlayer_exp_cnt,\n" +
                "                 onetothree_vv floatlayer_vv,\n" +
                "                 videochn_expose videochann_exp_cnt,\n" +
                "                 videochn_click videochann_click_cnt,\n" +
                "                 kddaily_expose infkddaily_exp_cnt,\n" +
                "                 kddaily_click infkddaily_click_cnt,\n" +
                "                 video_vv total_vv,\n" +
                "                 valid_vv valid_vv,\n" +
                "                 dianzan_cnt like_cnt,\n" +
                "                 share_cnt share_cnt,\n" +
                "                 biu_cnt biu_cnt,\n" +
                "                 main_comment mainclass_cmt_cnt,\n" +
                "                 main_comment_zan mainclass_cmtlike_cnt,\n" +
                "                 sub_comment sub2class_cmt_cnt,\n" +
                "                 sub_comment_zan sub2class_like_cnt,\n" +
                "                 NULL collect_cnt,\n" +
                "                      NULL accuse_cnt,\n" +
                "                           NULL negfeeback_cnt,\n" +
                "                                CAST( FLOOR(tot_watch_duration) AS BIGINT ) AS duration_video\n" +
                "FROM sng_mediaaccount_app :: kandian_video_medium_full_info_d\n" +
                "WHERE ftime = 20200607;";

        String str2 = "INSERT overwrite table kandian_video_medium_full_info_d partition(ftime = 20200607)\n" +
                "SELECT \n" +
                "    20200607 AS  ftime\n" +
                "    ,aa.rowkey\n" +
                "    ,bb.puin\n" +
                "    ,NVL(bb.short_v,0) AS video_type\n" +
                "    ,aa.video_vv\n" +
                "    ,aa.valid_vv\n" +
                "    ,aa.baoguang_cnt\n" +
                "    ,aa.dianzan_cnt\n" +
                "    ,aa.share_cnt\n" +
                "    ,aa.biu_cnt\n" +
                "    ,bb.bid\n" +
                "    ,bb.puin_name\n" +
                "    ,bb.upt\n" +
                "    ,bb.input_ts\n" +
                "    ,bb.asn_enable_time\n" +
                "    ,bb.kd_chann_new\n" +
                "    ,bb.kd_sec_chann_new\n" +
                "    ,bb.kd_trd_chann_new\n" +
                "    ,bb.time\n" +
                "    ,bb.ex_is_firstpubed\n" +
                "    ,bb.ex_is_ori\n" +
                "    ,bb.ex_is_exclusive\n" +
                "    ,bb.star_grade\n" +
                "    ,bb.ex_is_recom_edit\n" +
                "    ,bb.ex_explosions\n" +
                "    ,bb.st_vert\n" +
                "    ,bb.platform_src\n" +
                "    ,bb.st_src\n" +
                "    ,bb.crawl_src\n" +
                "    ,bb.kd_tag\n" +
                "    ,bb.st_txt_vid\n" +
                "    ,bb.st_txt\n" +
                "    ,bb.title AS  title\n" +
                "    ,bb.st_kd\n" +
                "    ,bb.st_sync\n" +
                "    ,bb.ex_premium\n" +
                "    ,bb.ex_is_choice\n" +
                "    ,bb.ex_is_day_better\n" +
                "    ,bb.output_ts\n" +
                "    ,aa.vv_weishi\n" +
                "    ,aa.comment_cnt\n" +
                "    ,url\n" +
                "    ,main_click\n" +
                "    ,kddaily_expose\n" +
                "    ,kddaily_click\n" +
                "    ,videochn_expose\n" +
                "    ,videochn_click\n" +
                "    ,otherchn_expose\n" +
                "    ,otherchn_click\n" +
                "    ,main_comment\n" +
                "    ,sub_comment\n" +
                "    ,main_comment_zan\n" +
                "    ,sub_comment_zan\n" +
                "    ,onetothree_expose\n" +
                "    ,onetothree_vv\n" +
                "    ,tot_expose\n" +
                "    ,watchduration\n" +
                "    ,union_chann\n" +
                "    ,union_chann_id\n" +
                "    ,union_sec_chann\n" +
                "    ,union_sec_chann_id\n" +
                "    ,union_trd_chann\n" +
                "    ,union_trd_chann_id\n" +
                "    ,union_tag\n" +
                "    ,union_tag_id \n" +
                "    ,tot_watch_duration\n" +
                "from \n" +
                "(\n" +
                "    select\n" +
                "        rowkey\n" +
                "        ,vv as video_vv\n" +
                "        ,vv_inc_wesee as vv_weishi\n" +
                "        ,valid_vv\n" +
                "        ,baoguang_cnt\n" +
                "        ,dianzan_cnt\n" +
                "        ,share_cnt\n" +
                "        ,biu_cnt\n" +
                "        ,comment_cnt\n" +
                "        ,main_click\n" +
                "        ,kddaily_expose\n" +
                "        ,kddaily_click\n" +
                "        ,videochn_expose\n" +
                "        ,videochn_click\n" +
                "        ,otherchn_expose\n" +
                "        ,otherchn_click\n" +
                "        ,main_comment\n" +
                "        ,sub_comment\n" +
                "        ,main_comment_zan\n" +
                "        ,sub_comment_zan\n" +
                "        ,onetothree_expose\n" +
                "        ,onetothree_vv\n" +
                "        ,(nvl(baoguang_cnt,0)+nvl(kddaily_expose,0)+nvl(videochn_expose,0)+nvl(otherchn_expose,0)+nvl(onetothree_expose,0)) AS tot_expose\n" +
                "        ,watchduration\n" +
                "        ,(watchduration * vv) / 1000 AS tot_watch_duration\n" +
                "    from kandian_mid_video_cinfo_d where ftime = 20200607\n" +
                ")aa\n" +
                "\n" +
                "left join\n" +
                "    \n" +
                "(\n" +
                "    select \n" +
                "        rowkey\n" +
                "        ,puin\n" +
                "        ,bid\n" +
                "        ,regexp_replace(puin_name,'[\\t|\\n|\\r]+','') as puin_name\n" +
                "        ,short_v\n" +
                "        ,upt\n" +
                "        ,input_ts\n" +
                "        ,asn_enable_time\n" +
                "        ,kd_chann_new\n" +
                "        ,kd_sec_chann_new\n" +
                "        ,kd_trd_chann_new\n" +
                "        ,time\n" +
                "        ,ex_is_firstpubed\n" +
                "        ,ex_is_ori\n" +
                "        ,ex_is_exclusive\n" +
                "        ,star_grade\n" +
                "        ,ex_is_recom_edit\n" +
                "        ,ex_explosions\n" +
                "        ,st_vert\n" +
                "        ,platform_src\n" +
                "        ,st_src\n" +
                "        ,crawl_src\n" +
                "        ,kd_tag\n" +
                "        ,st_txt_vid\n" +
                "        ,st_txt\n" +
                "        ,regexp_replace(unbase64(title),'[\\t|\\n|\\r]+','') as title\n" +
                "        ,st_kd\n" +
                "        ,st_sync\n" +
                "        ,ex_premium\n" +
                "        ,ex_is_choice\n" +
                "        ,ex_is_day_better\n" +
                "        ,output_ts\n" +
                "        ,url\n" +
                "        ,union_chann\n" +
                "        ,union_chann_id\n" +
                "        ,union_sec_chann\n" +
                "        ,union_sec_chann_id\n" +
                "        ,union_trd_chann\n" +
                "        ,union_trd_chann_id\n" +
                "        ,union_tag\n" +
                "        ,union_tag_id             \n" +
                " from sng_mp_etldata::t_kd_video_info \n" +
                " where f_date = 20200607\n" +
                " ) bb \n" +
                "on aa.rowkey = bb.rowkey \n" +
                "distribute by ftime;";

        String str3 = "INSERT OVERWRITE TABLE kandian_mid_video_cinfo_d PARTITION(ftime = 20200607)\n" +
                "    SELECT\n" +
                "        20200607 as ftime\n" +
                "        ,rowkey\n" +
                "        ,NVL(SUM(main_expose),0)\n" +
                "        ,NVL(SUM(tot_vv),0)\n" +
                "        ,NVL(SUM(tot_vv_incwesee),0)\n" +
                "        ,NVL(SUM(main_click),0)\n" +
                "        ,NVL(SUM(kddaily_expose),0)\n" +
                "        ,NVL(SUM(kddaily_click),0)\n" +
                "        ,NVL(SUM(tot_comment),0)\n" +
                "        ,NVL(SUM(dianzan),0)\n" +
                "        ,NVL(SUM(share),0)\n" +
                "        ,NVL(SUM(biu_bak),0)\n" +
                "        ,NVL(SUM(accuse),0)\n" +
                "        ,NVL(SUM(collect),0)\n" +
                "        ,NVL(SUM(comment_zan),0)\n" +
                "        ,NVL(SUM(main_ratio),0)\n" +
                "        ,NVL(SUM(kddaily_ratio),0)\n" +
                "        ,NVL(SUM(videochn_expose),0)\n" +
                "        ,NVL(SUM(videochn_click),0)\n" +
                "        ,NVL(SUM(videochn_ratio),0)\n" +
                "        ,NVL(SUM(otherchn_expose),0)\n" +
                "        ,NVL(SUM(otherchn_click),0)\n" +
                "        ,NVL(SUM(otherchn_ratio),0)\n" +
                "        ,NVL(SUM(main_comment),0)\n" +
                "        ,NVL(SUM(sub_comment),0)\n" +
                "        ,NVL(SUM(main_comment_zan),0)\n" +
                "        ,NVL(SUM(sub_comment_zan),0)\n" +
                "        ,NVL(SUM(cancel_dianzan),0)\n" +
                "        ,NVL(SUM(onetothree_expose),0)\n" +
                "        ,NVL(SUM(onetothree_vv),0)\n" +
                "        ,NVL(SUM(mediaduration),0)\n" +
                "        ,NVL(SUM(watchduration),0)\n" +
                "        ,NVL(SUM(watchratio),0)\n" +
                "        ,NVL(SUM(valid_ratio),0)\n" +
                "        ,NVL(SUM(all_neg_fback),0)\n" +
                "        ,NVL(SUM(onetothree_neg_fback),0)\n" +
                "        ,NVL(SUM(feeds_neg_fback),0)\n" +
                "        ,NVL(SUM(valid_vv),0) AS valid_vv\n" +
                "    FROM\n" +
                "        (\n" +
                "            SELECT a.* FROM\n" +
                "            (\n" +
                "                SELECT\n" +
                "                  rowkey\n" +
                "                  ,SUM(main_expose) AS main_expose\n" +
                "                  ,SUM(main_click) AS main_click\n" +
                "                  ,SUM(main_ratio) AS main_ratio\n" +
                "                  ,SUM(kddaily_expose) AS  kddaily_expose -- 20191022新口径\n" +
                "                  ,SUM(kddaily_click) AS kddaily_click -- 20191022新口径\n" +
                "                  ,SUM(kddaily_ratio) AS kddaily_ratio\n" +
                "                  ,SUM(videochn_expose) AS videochn_expose\n" +
                "                  ,SUM(videochn_click) AS videochn_click\n" +
                "                  ,SUM(videochn_ratio) AS videochn_ratio\n" +
                "                  ,SUM(otherchn_expose) AS otherchn_expose\n" +
                "                  ,SUM(otherchn_click) AS otherchn_click\n" +
                "                  ,SUM(otherchn_ratio) AS otherchn_ratio\n" +
                "                  ,SUM(comment_zan) AS comment_zan\n" +
                "                  ,SUM(main_comment) AS main_comment\n" +
                "                  ,SUM(sub_comment) AS sub_comment\n" +
                "                  ,SUM(tot_comment) AS tot_comment\n" +
                "                  ,SUM(main_comment_zan) AS main_comment_zan\n" +
                "                  ,SUM(sub_comment_zan) AS sub_comment_zan\n" +
                "                  ,NULL AS onetothree_expose\n" +
                "                  ,NULL AS onetothree_vv\n" +
                "                  ,NULL AS tot_vv\n" +
                "                  ,NULL AS tot_vv_incwesee\n" +
                "                  ,NULL AS mediaduration\n" +
                "                  ,NULL AS watchduration\n" +
                "                  ,NULL AS watchratio\n" +
                "                  ,NULL AS valid_ratio\n" +
                "                  ,NULL AS dianzan\n" +
                "                  ,NULL AS cancel_dianzan\n" +
                "                  ,SUM(biu_bak) AS biu_bak\n" +
                "                  ,NULL AS share\n" +
                "                  ,NULL AS collect\n" +
                "                  ,NULL AS accuse\n" +
                "                  ,NULL AS all_neg_fback\n" +
                "                  ,NULL AS onetothree_neg_fback\n" +
                "                  ,NULL AS feeds_neg_fback\n" +
                "                  ,NULL AS valid_vv\n" +
                "                FROM\n" +
                "                (\n" +
                "                   SELECT\n" +
                "                      rowkey\n" +
                "                      ,main_expose\n" +
                "                      ,main_click\n" +
                "                      ,main_ratio\n" +
                "                      ,kddaily_expose\n" +
                "                      ,kddaily_click\n" +
                "                      ,kddaily_ratio\n" +
                "                      ,videochn_expose\n" +
                "                      ,videochn_click\n" +
                "                      ,videochn_ratio\n" +
                "                      ,otherchn_expose\n" +
                "                      ,otherchn_click\n" +
                "                      ,otherchn_ratio\n" +
                "                      ,NULL AS comment_zan\n" +
                "                      ,NULL AS main_comment\n" +
                "                      ,NULL AS sub_comment\n" +
                "                      ,NULL AS tot_comment\n" +
                "                      ,NULL AS main_comment_zan\n" +
                "                      ,NULL AS sub_comment_zan\n" +
                "                      ,NULL AS biu_bak\n" +
                "                      ,NULL AS new_ribao_expose\n" +
                "                      ,NULL AS new_ribao_click\n" +
                "                   FROM kandian_ods_all_feeds_expclick_d\n" +
                "                   WHERE ftime = 20200607\n" +
                "\n" +
                "                   UNION ALL\n" +
                "\n" +
                "                   SELECT\n" +
                "                      rowkey\n" +
                "                      ,NULL AS  main_expose\n" +
                "                      ,NULL AS  main_click\n" +
                "                      ,NULL AS  main_ratio\n" +
                "                      ,NULL AS  kddaily_expose\n" +
                "                      ,NULL AS  kddaily_click\n" +
                "                      ,NULL AS  kddaily_ratio\n" +
                "                      ,NULL AS  videochn_expose\n" +
                "                      ,NULL AS  videochn_click\n" +
                "                      ,NULL AS  videochn_ratio\n" +
                "                      ,NULL AS  otherchn_expose\n" +
                "                      ,NULL AS  otherchn_click\n" +
                "                      ,NULL AS  otherchn_ratio\n" +
                "                      ,comment_zan\n" +
                "                      ,main_comment\n" +
                "                      ,sub_comment\n" +
                "                      ,tot_comment\n" +
                "                      ,main_comment_zan\n" +
                "                      ,sub_comment_zan\n" +
                "                      ,NULL AS biu_bak\n" +
                "                      ,NULL AS new_ribao_expose\n" +
                "                      ,NULL AS new_ribao_click\n" +
                "                    FROM kandian_ods_all_comment_comzan_d\n" +
                "                    where ftime = 20200607\n" +
                "\n" +
                "                  UNION ALL\n" +
                "\n" +
                "                  SELECT\n" +
                "                     rowkey\n" +
                "                     ,NULL AS main_expose\n" +
                "                     ,NULL AS main_click\n" +
                "                     ,NULL AS main_ratio\n" +
                "                     ,NULL AS kddaily_expose\n" +
                "                     ,NULL AS kddaily_click\n" +
                "                     ,NULL AS kddaily_ratio\n" +
                "                     ,NULL AS videochn_expose\n" +
                "                     ,NULL AS videochn_click\n" +
                "                     ,NULL AS videochn_ratio\n" +
                "                     ,NULL AS otherchn_expose\n" +
                "                     ,NULL AS otherchn_click\n" +
                "                     ,NULL AS otherchn_ratio\n" +
                "                     ,NULL AS comment_zan\n" +
                "                     ,NULL AS main_comment\n" +
                "                     ,NULL AS sub_comment\n" +
                "                     ,NULL AS tot_comment\n" +
                "                     ,NULL AS main_comment_zan\n" +
                "                     ,NULL AS sub_comment_zan\n" +
                "                     ,publish_biu AS biu_bak\n" +
                "                     ,NULL AS new_ribao_expose\n" +
                "                     ,NULL AS new_ribao_click\n" +
                "                  FROM kandian_ods_all_biu_d\n" +
                "                  where ftime = 20200607\n" +
                "\n" +
                "\n" +
                "                  UNION ALL\n" +
                "\n" +
                "                  SELECT\n" +
                "                     rowkey\n" +
                "                     ,NULL AS main_expose\n" +
                "                     ,NULL AS main_click\n" +
                "                     ,NULL AS main_ratio\n" +
                "                     ,NULL AS kddaily_expose\n" +
                "                     ,NULL AS kddaily_click\n" +
                "                     ,NULL AS kddaily_ratio\n" +
                "                     ,NULL AS videochn_expose\n" +
                "                     ,NULL AS videochn_click\n" +
                "                     ,NULL AS videochn_ratio\n" +
                "                     ,NULL AS otherchn_expose\n" +
                "                     ,NULL AS otherchn_click\n" +
                "                     ,NULL AS otherchn_ratio\n" +
                "                     ,NULL AS comment_zan\n" +
                "                     ,NULL AS main_comment\n" +
                "                     ,NULL AS sub_comment\n" +
                "                     ,NULL AS tot_comment\n" +
                "                     ,NULL AS main_comment_zan\n" +
                "                     ,NULL AS sub_comment_zan\n" +
                "                     ,NULL AS biu_bak\n" +
                "                     ,new_ribao_expose\n" +
                "                     ,new_ribao_click\n" +
                "                  FROM kandian_ods_all_ribao_feeds_d\n" +
                "                  WHERE ftime = 20200607\n" +
                "\n" +
                "\n" +
                "                  )\n" +
                "                GROUP BY rowkey\n" +
                "            )a\n" +
                "\n" +
                "          JOIN\n" +
                "\n" +
                "          (\n" +
                "            SELECT\n" +
                "                rowkey\n" +
                "            FROM sng_mp_etldata::t_kd_video_info\n" +
                "            WHERE f_date = 20200607\n" +
                "          )b\n" +
                "          on a.rowkey = b.rowkey\n" +
                "\n" +
                "          UNION ALL\n" +
                "\n" +
                "          SELECT\n" +
                "             rowkey\n" +
                "             ,NULL AS main_expose\n" +
                "             ,NULL AS main_click\n" +
                "             ,NULL AS main_ratio\n" +
                "             ,NULL AS kddaily_expose\n" +
                "             ,NULL AS kddaily_click\n" +
                "             ,NULL AS kddaily_ratio\n" +
                "             ,NULL AS videochn_expose\n" +
                "             ,NULL AS videochn_click\n" +
                "             ,NULL AS videochn_ratio\n" +
                "             ,NULL AS otherchn_expose\n" +
                "             ,NULL AS otherchn_click\n" +
                "             ,NULL AS otherchn_ratio\n" +
                "             ,NULL AS comment_zan\n" +
                "             ,NULL AS main_comment\n" +
                "             ,NULL AS sub_comment\n" +
                "             ,NULL AS tot_comment\n" +
                "             ,NULL AS main_comment_zan\n" +
                "             ,NULL AS sub_comment_zan\n" +
                "             ,onetothree_expose\n" +
                "             ,onetothree_vv\n" +
                "             ,tot_vv\n" +
                "             ,tot_vv_incwesee\n" +
                "             ,mediaduration\n" +
                "             ,watchduration\n" +
                "             ,watchratio\n" +
                "             ,valid_ratio\n" +
                "             ,NULL AS dianzan\n" +
                "             ,NULL AS cancel_dianzan\n" +
                "             ,NULL AS biu_bak\n" +
                "             ,NULL AS share\n" +
                "             ,NULL AS collect\n" +
                "             ,NULL AS accuse\n" +
                "             ,NULL AS all_neg_fback\n" +
                "             ,NULL AS onetothree_neg_fback\n" +
                "             ,NULL AS feeds_neg_fback\n" +
                "             ,valid_vv\n" +
                "          from kandian_ods_video_core_d\n" +
                "          where ftime = 20200607\n" +
                "\n" +
                "          UNION ALL\n" +
                "\n" +
                "          SELECT\n" +
                "             rowkey\n" +
                "             ,NULL AS main_expose\n" +
                "             ,NULL AS main_click\n" +
                "             ,NULL AS main_ratio\n" +
                "             ,NULL AS kddaily_expose\n" +
                "             ,NULL AS kddaily_click\n" +
                "             ,NULL AS kddaily_ratio\n" +
                "             ,NULL AS videochn_expose\n" +
                "             ,NULL AS videochn_click\n" +
                "             ,NULL AS videochn_ratio\n" +
                "             ,NULL AS otherchn_expose\n" +
                "             ,NULL AS otherchn_click\n" +
                "             ,NULL AS otherchn_ratio\n" +
                "             ,NULL AS comment_zan\n" +
                "             ,NULL AS main_comment\n" +
                "             ,NULL AS sub_comment\n" +
                "             ,NULL AS tot_comment\n" +
                "             ,NULL AS main_comment_zan\n" +
                "             ,NULL AS sub_comment_zan\n" +
                "             ,NULL AS onetothree_expose\n" +
                "             ,NULL AS onetothree_vv\n" +
                "             ,NULL AS tot_vv\n" +
                "             ,NULL AS tot_vv_incwesee\n" +
                "             ,NULL AS mediaduration\n" +
                "             ,NULL AS watchduration\n" +
                "             ,NULL AS watchratio\n" +
                "             ,NULL AS valid_ratio\n" +
                "             ,dianzan\n" +
                "             ,cancel_dianzan\n" +
                "             ,biu_bak\n" +
                "             ,share\n" +
                "             ,collect\n" +
                "             ,accuse\n" +
                "             ,NULL AS all_neg_fback\n" +
                "             ,NULL AS onetothree_neg_fback\n" +
                "             ,NULL AS feeds_neg_fback\n" +
                "             ,NULL AS valid_vv\n" +
                "          from kandian_ods_video_interactive_d\n" +
                "          where ftime = 20200607\n" +
                "\n" +
                "          UNION ALL\n" +
                "\n" +
                "          SELECT\n" +
                "             rowkey\n" +
                "             ,NULL AS main_expose\n" +
                "             ,NULL AS main_click\n" +
                "             ,NULL AS main_ratio\n" +
                "             ,NULL AS kddaily_expose\n" +
                "             ,NULL AS kddaily_click\n" +
                "             ,NULL AS kddaily_ratio\n" +
                "             ,NULL AS videochn_expose\n" +
                "             ,NULL AS videochn_click\n" +
                "             ,NULL AS videochn_ratio\n" +
                "             ,NULL AS otherchn_expose\n" +
                "             ,NULL AS otherchn_click\n" +
                "             ,NULL AS otherchn_ratio\n" +
                "             ,NULL AS comment_zan\n" +
                "             ,NULL AS main_comment\n" +
                "             ,NULL AS sub_comment\n" +
                "             ,NULL AS tot_comment\n" +
                "             ,NULL AS main_comment_zan\n" +
                "             ,NULL AS sub_comment_zan\n" +
                "             ,NULL AS onetothree_expose\n" +
                "             ,NULL AS onetothree_vv\n" +
                "             ,NULL AS tot_vv\n" +
                "             ,NULL AS tot_vv_incwesee\n" +
                "             ,NULL AS mediaduration\n" +
                "             ,NULL AS watchduration\n" +
                "             ,NULL AS watchratio\n" +
                "             ,NULL AS valid_ratio\n" +
                "             ,NULL AS dianzan\n" +
                "             ,NULL AS cancel_dianzan\n" +
                "             ,NULL AS biu_bak\n" +
                "             ,NULL AS share\n" +
                "             ,NULL AS collect\n" +
                "             ,NULL AS accuse\n" +
                "             ,all_neg_fback\n" +
                "             ,onetothree_neg_fback\n" +
                "             ,feeds_neg_fback\n" +
                "             ,NULL AS valid_vv\n" +
                "          from kandian_ods_video_neg_feedback_d\n" +
                "          where ftime = 20200607\n" +
                "        )\n" +
                "        group by rowkey\n" +
                "        distribute by ftime;";

        String str4="insert overwrite table dw_layers_remain__final_shortFlowRanker partition(statis_time=20200613) select * from ( select 20200613 as statis_time , coalesce(a.bucketid,b.bucketid,c.bucketid) as bucket_id ,sum(if(a.uin is not null and b.uin is not null,1,0)) as uin_nominator1 ,sum(if(a.uin is not null and b.uin is not null,1,0))/sum(if(b.uin is not null,1,0)) as remain_ratio1 ,sum(if(a.uin is not null and c.uin is not null,1,0)) as uin_nominator7 ,sum(if(a.uin is not null and c.uin is not null,1,0))/sum(if(c.uin is not null,1,0)) as remain_ratio7 from (select bucketid,uin from dw_layers_remain__bucket_shortFlowRanker where statis_time = 20200613 ) a full outer join (select bucketid,uin from dw_layers_remain__bucket_shortFlowRanker where statis_time = 20200612 ) b on a.uin = b.uin full outer join (select bucketid,uin from dw_layers_remain__bucket_shortFlowRanker where statis_time = 20200606 ) c on a.uin = c.uin group by coalesce(a.bucketid,b.bucketid,c.bucketid) )t distribute by 1";




        String sql = str4;

        System.out.println(sqlTF(sql));
    }
}