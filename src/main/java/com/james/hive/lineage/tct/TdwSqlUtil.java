package com.james.hive.lineage.tct;

import com.google.gson.Gson;
import com.james.hive.lineage.tct.entity.ColsInfo;
import com.james.hive.lineage.tct.entity.RetObj;
import com.james.hive.lineage.tct.entity.TableInfo;
import com.james.hive.lineage.tct.entity.TdwTableInfo;
import com.james.hive.utils.BaseResonseErrorHandler;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TdwSqlUtil {
    private static String[] tdwDBArr = {"pcg_txkd_qb_info_app",
            "sng_publicaccount_result",
            "omg_mobile_newsdev",
            "sng_mediaaccount_app",
            "omg_prs_dw",
            "default_db",
            "sng_mp_etldata",
            "mp_portal_result",
            "sng_publicaccount_msgclickreport",
            "sng_cp_fact",
            "SNG_tdbank",
            "hlw",
            "hlw_tencentvideo",
            "omg_newsapp_mid",
            "sng_cgi_oidb_quality_monitor",
            "sng_mp_cdbdata",
            "sng_kandian_social_app",
            "omg_prs_dm",
            "sng_qqkd_ml_app",
            "omg_tdbank",
            "pcg_txkd_shared_data_app",
            "bg_tkd_ug_app",
            "test",
            "omg_newsapp",
            "d_p2p_log"};

    private static List<String> pattern;
    private static List<String> replace;

    private static void init() {

        pattern = new ArrayList<String>();
        replace = new ArrayList<String>();

        pattern.add("(?i)INSERT\\s+TABLE");
        replace.add("INSERT INTO");

        pattern.add("(?i)INSERT\\s+OVERWRITE\\s+TABLE");
        replace.add("INSERT INTO");

        pattern.add("(?i)INSERT\\s+OVERWRITE\\s+INTO\\s+TABLE");
        replace.add("INSERT INTO");

        pattern.add("::");
        replace.add(".");

        pattern.add("\\\\t");
        replace.add("TOK_BACKSLASH_T");

        pattern.add("\\\\r");
        replace.add("TOK_BACKSLASH_R");

        pattern.add("\\\\n");
        replace.add("TOK_BACKSLASH_N");

        pattern.add("(?i)partition\\(\\s*.+?\\s*\\)");
        replace.add(" ");

        pattern.add("distribute\\s+by\\s+.+");
        replace.add(";");

        // 消除sql中的注释
        pattern.add("(/\\*([^*]|[\\r\\n]|(\\*+([^*/]|[\\r\\n])))*\\*+/)|'(?:[^']|'')*'|(--.*)");
        replace.add(" ");
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
     * 对group by 前面的临时表起别名
     *
     * @param str
     * @return
     */
    public static String addAlias(String str) {
        String pattern = "\\)\\s+group\\s+by";
        str = str.replaceAll(pattern, ") " + randomABC() + " group by");
        return str;
    }

    /**
     * 随机生成5个字母
     *
     * @return
     */

    public static String randomABC() {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 5; i++) {
            int number = random.nextInt(52);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    /**
     * 主方法
     *
     * @param str 传入要被转换字符串
     */
    public static String sqlTF(String str) {
        init();
        for (int i = 0; i < pattern.size(); i++) {
            str = str.replaceAll(pattern.get(i), replace.get(i));
        }
        str = unbase64TF(str);
        str = addAlias(str);
        return str;
    }

    public static ResponseEntity<String> reqTdw(String f_db_name, String f_table_name) {
        String url = "http://10.222.106.18/UserPrivilegeInternalService";
        String m = "queryTableInfoNoPartitionExternal";
        String p = "[\"tl\",\"tdw_jamesqjiang\",\"tdw_jamesqjiang\",\"" + f_db_name + "\",\"" + f_table_name + "\"]";

        RestTemplate client = new RestTemplate();
        ((SimpleClientHttpRequestFactory) client.getRequestFactory()).setReadTimeout(1000 * 10);
        client.setErrorHandler(new BaseResonseErrorHandler());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("m", m)
                .queryParam("p", p);
        HttpEntity entity = new HttpEntity(headers);
        ResponseEntity<String> response;
        try {
            response = client.exchange(
                    java.net.URLDecoder.decode(builder.toUriString(), "ISO-8859-1"),
                    HttpMethod.GET,
                    entity,
                    String.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return response;
    }

    public static String makeTdwTableDdl(String tdwDB, String tdwTable) throws Exception {
        if (null == tdwDB || null == tdwTable) {
            return null;
        }

        final String txkdDBPrefix = "use txkd_dc_insert_db;";
        final String dbPrefix = "create database if not exists " + tdwDB + "; use " + tdwDB + ";";
        final String dllPrefix = "CREATE TABLE if not exists " + tdwTable + "( ";
        final String ddlPostfix = " );";

        ResponseEntity<String> queryResult = reqTdw(tdwDB, tdwTable);

        String httpBodyTmp = queryResult.getBody();
        String httpBody = new String(httpBodyTmp.getBytes("ISO-8859-1"), "UTF-8");
//        System.out.println(httpBody);
//        System.out.println("-----------------------------------------------------------------------------------");

        Gson gson = new Gson();
        TdwTableInfo tdwTableInfo = gson.fromJson(httpBody, TdwTableInfo.class);

        RetObj retObj = tdwTableInfo.getRetObj();

        TableInfo tableInfo = retObj.getTableInfo();

        StringBuilder sb = new StringBuilder();
        List<ColsInfo> colsInfoList = tableInfo.getColsInfo();
        for (ColsInfo c : colsInfoList) {
//            System.out.println(c.getColName() + " -> " + c.getColType() + " -> " + c.getColComent());
            sb.append(c.getColName() + " " + c.getColType() + ",");
        }

        String sbString = sb.toString();
//        System.out.println(String.format("sbString=%s", sbString));

        String ddl = dbPrefix + dllPrefix + sbString.substring(0, sbString.length() - 1) + ddlPostfix;
        // 旁路在txkd_dc_insert_db库中创建同名数据表
        ddl = ddl + txkdDBPrefix + dllPrefix + sbString.substring(0, sbString.length() - 1) + ddlPostfix;

        System.out.println(String.format(">>> db=%s table=%s", tdwDB, tdwTable));
        System.out.println(String.format("%s", ddl));
        return ddl;
    }

    public static String tryMakeTdwTableDdl(String tdwTable) {
        if (null == tdwTable || tdwTable.isEmpty()) {
            return null;
        }

        String tdwTableDDL = null;

        for (int i = 0; i < tdwDBArr.length; i++) {
            System.out.println("DB: " + tdwDBArr[i]);
            try {
                tdwTableDDL = makeTdwTableDdl(tdwDBArr[i], tdwTable);
                System.out.println(String.format(">>> tdwTableDDL=%s", tdwTableDDL));
                if (null != tdwTableDDL) {
                    break;
                }
            } catch (Exception e) {
                System.out.println("!!! " + tdwTable + " -> " + e.getMessage());
            }
        }

        return tdwTableDDL;
    }

    public static List<String> tryMakeTdwTableDdls(Set<String> tdwTables) {
        if (null == tdwTables || tdwTables.size() == 0) {
            return null;
        }

        List<String> tdwTableDdlList = new ArrayList<>();
        for (String tdwTable : tdwTables) {
            String tdwTableDDL = null;

            for (int i = 0; i < tdwDBArr.length; i++) {
                try {
                    tdwTableDDL = makeTdwTableDdl(tdwDBArr[i], tdwTable);
                    if (null != tdwTableDDL) {
                        tdwTableDdlList.add(tdwTableDDL);
                    }
                } catch (Exception e) {
                    System.out.println("!!! " + tdwTable + " -> " + e.getMessage());
                }
            }
        }

        return tdwTableDdlList;
    }

    public static void main(String[] args) {
//        String s1 = "INSERT OVERWRITE TABLE rikideng_DWD_small_scrip_session_talk PARTITION(ftime = 20200613) SELECT 20200613 AS ftime, op_type, if(op_type != SINGLE_QUOTE0X800A806SINGLE_QUOTE,d1,NULL) AS rowkey, d2 AS message_id, fromuin AS from_uin, touin AS to_uin FROM hlw::t_dw_dc01160 WHERE op_type IN (SINGLE_QUOTE0X800A805SINGLE_QUOTE, SINGLE_QUOTE0X800A806SINGLE_QUOTE, SINGLE_QUOTE0X800A80ESINGLE_QUOTE) AND tdbank_imp_date BETWEEN 2020061300 AND 2020061323";
//        String s2 = "INSERT OVERWRITE TABLE t_dwa_kd_video_hudong_ald_layer_1d_d PARTITION(ftime=20200613) SELECT 20200613 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) ,SUM(watch_puin_pv) ,SUM(watch_puin_pv_o2t) ,SUM(watch_puin_pv_ld) ,SUM(puin_page_pv) ,SUM(puin_page_pv_02t) ,SUM(puin_page_pv_ld) ,SUM(zan_pv) ,SUM(zan_pv_o2t) ,SUM(zan_pv_ld) ,SUM(d_zan_pv) ,SUM(d_zan_pv_o2t) ,SUM(d_zan_pv_ld) ,SUM(comment_pv) ,SUM(comment_pv_o2t) ,SUM(comment_pv_ld) ,SUM(biu_pv) ,SUM(biu_pv_o2t) ,SUM(biu_pv_ld) ,SUM(q_comment_pv) ,SUM(q_comment_pv_o2t) ,SUM(q_comment_pv_ld) ,SUM(comment_duration) FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEvideo_hudongSINGLE_QUOTE ) a RIGHT JOIN t_dwa_kd_video_hudong_ald_dwa_1d_d partition (p_20200613) b ON a.dt=b.ftime GROUP BY group_key ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";
//        String s3 = "insert table kuaibao_ods_boss4715_log select 20200613 as imp_date, ftime, idfv, sos, shardware, logintype, openid, qq, phone_id, teamid, teamname, sidfv, binarymd5, copname, sappver, iisjb from omg_tdbank::mobile_t_boss_v1_4715 where tdbank_imp_date >= 2020061300 and tdbank_imp_date <= 2020061324\n";
//        String s4 = "INSERT OVERWRITE TABLE dwa_kd_daily_infite_ald_layer_1h_h PARTITION(ftime=2020061420) SELECT /*+ MAPJOIN(a)*/ 2020061420 ,group_key ,SINGLE_QUOTEallSINGLE_QUOTE as utype ,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num) as bucket_id ,SUM(daily_infite_tab_expose_pv) AS daily_unbounded_tab_exp_pv ,COUNT(IF(daily_infite_tab_expose_pv>0,uin,null)) AS daily_unbounded_tab_exp_uv ,SUM(daily_infite_tab_click_pv) AS daily_unbounded_tab_clk_pv ,COUNT(IF(daily_infite_tab_click_pv>0,uin,null)) AS daily_unbounded_tab_clk_uv ,SUM(daily_infite_expose_pv) AS daily_unbounded_exp_pv ,COUNT(IF(daily_infite_expose_pv>0,uin,null)) AS daily_unbounded_exp_uv ,SUM(daily_infite_click_pv) AS daily_unbounded_clk_pv ,COUNT(IF(daily_infite_click_pv>0,uin,null)) AS daily_unbounded_clk_uv ,COUNT(IF(daily_infite_dau3a>0,uin,null)) AS daily_unbounded_dau3a ,SUM(daily_infite_dur) AS daily_unbounded_duration ,COUNT(IF(daily_infite_dur>0,uin,null)) AS daily_unbounded_duration_uv ,SUM(daily_infite_pv) AS daily_unbounded_pic_pv ,COUNT(IF(daily_infite_pv>0,uin,null)) AS daily_unbounded_pic_uv ,SUM(daily_infite_vv) AS daily_unbounded_video_pv ,COUNT(IF(daily_infite_vv>0,uin,null)) AS daily_unbounded_video_uv FROM ( SELECT dt ,bucket_num ,group_key FROM mengqi_tpg_ald_info WHERE dt=20200613 AND valid=1 AND data_type=SINGLE_QUOTEhour_reportSINGLE_QUOTE) a RIGHT JOIN dwa_kd_daily_infite_ald_dwa_1h_h partition (p_2020061420) b on a.dt=20200613 where b.ftime=2020061420 group by group_key,MOD(bigint1(conv(substr(md5(uin&#124;&#124;SINGLE_QUOTE-SINGLE_QUOTE,group_key),0,15),16,10)),bucket_num)";
//        String s5 = "INSERT OVERWRITE TABLE dim_kb_newald_layer_vaild__info PARTITION(ftime=20200613) select 20200613,layer_id,experiment_id from dim_kb_newald_layer_start_exp_info where ftime=20200613 and crt_time < 20200613 and exp_dt <20200613 and exp_dt is not null\n";
//        String s6 = "insert table sng_mp_etldata::t_dwa_asn_audit_summarynew_d SELECT 20200613 AS p_date, vendor, f_staff_id, dim, sum(nvl(value,0)) AS value, '' AS ext FROM (SELECT vendor, t_a.f_staff_id, dim, value FROM (SELECT f_staff_id, dim, sum(nvl(value,0)) AS value, '' AS ext FROM (SELECT f_staff_id, '短视频禁用数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='短视频' AND f_audit_status!=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频启用打tag不打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='短视频' AND f_audit_status=1 AND auto_use_tag=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频启用打tag打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='短视频' AND f_audit_status=1 AND auto_use_tag=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频启用不打tag打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='短视频' AND f_audit_status=1 AND auto_use_tag=0 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频启用不打tag不打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='短视频' AND f_audit_status=1 AND auto_use_tag=0 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频禁用数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='小视频' AND f_audit_status!=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频启用打tag不打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='小视频' AND f_audit_status=1 AND auto_use_tag=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频启用打tag打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='小视频' AND f_audit_status=1 AND auto_use_tag=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频启用不打tag打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='小视频' AND f_audit_status=1 AND auto_use_tag=0 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频启用不打tag不打分类属' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频启标审核' AND video_type='小视频' AND f_audit_status=1 AND auto_use_tag=0 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '图文启用数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('图文安全审核_微信跳微信落地页', '图文安全审核_微信新看点落地页', '图文-微信-微信企鹅号', '企鹅号标准_先发后审', '企鹅号标准_先审后发', '特殊渠道_辟谣', 'POP复核领单新-回捞审核池', '复核领单(新)_回捞审核池', '图文安全审核_微信（企鹅号内容）') AND f_audit_status=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '图文禁用数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('图文安全审核_微信跳微信落地页', '图文安全审核_微信新看点落地页', '图文-微信-微信企鹅号', '企鹅号标准_先发后审', '企鹅号标准_先审后发', '特殊渠道_辟谣', 'POP复核领单新-回捞审核池', '复核领单(新)_回捞审核池', '图文安全审核_微信（企鹅号内容）') AND f_audit_status!=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短内容总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('图文审核_短内容', '短内容-知乎问题', '特殊渠道_知乎回答', '图文安全审核_短内容-ugc短内容审核') GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '话题总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('互动安全审核_UGC审核_UGC话题-先审后发', '互动安全审核_UGC审核_UGC话题-先发后审') GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '问答总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='互动安全审核_UGC审核_UGC问答' GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '评论总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ( '互动安全审核_评论审核_先发后审', '互动安全审核_评论审核_先审后发') GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '图集总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('图文安全审核_图集审核（src49)', '图文审核_图集审核（强制启用内容）') GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '视频POP复核短视频数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('视频复核领单_爆款池', '视频浏览器高爆池', '视频复核领单_其他审核池', '视频随机抽检池', '视频复核（二次POP池）') AND video_type='短视频' GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '视频POP复核小视频数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool IN ('视频复核领单_爆款池', '视频浏览器高爆池', '视频复核领单_其他审核池', '视频随机抽检池', '视频复核（二次POP池）') AND video_type='小视频' GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '图文POP复核总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='复核领单(新)_热文复核' GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频回捞复核通过数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_回捞审核池' AND video_type='短视频' AND f_audit_status=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频回捞复核通过数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_回捞审核池' AND video_type='小视频' AND f_audit_status=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频回捞复核不通过数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_回捞审核池' AND video_type='短视频' AND f_audit_status!=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频回捞复核不通过数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_回捞审核池' AND video_type='小视频' AND f_audit_status!=1 GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '短视频分类复审总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_分类复审' AND video_type='短视频' GROUP BY f_staff_id UNION ALL SELECT f_staff_id, '小视频分类复审总数' AS dim, count(DISTINCT f_rowkey) AS value FROM t_ods_asn_audit_summarynew_d WHERE p_date=20200613 AND audit_pool='视频复核领单_分类复审' AND video_type='小视频' GROUP BY f_staff_id) A GROUP BY f_staff_id, dim) t_a LEFT JOIN (SELECT f_staff_id, max(vendor) vendor FROM (SELECT DISTINCT uid AS f_staff_id, CASE WHEN name rlike '.*\\_.*' THEN split(name,'\\_')[0] WHEN name rlike '.*-.*' THEN split(name,'-')[0] ELSE '其他' END AS vendor FROM t_dim_asn_auditor_daily WHERE f_date = 20200613 UNION ALL SELECT DISTINCT asn_audit_id AS f_staff_id, asn_audit_group AS vendor FROM t_lsa_article_center_hbase_merged_daily_migrate WHERE imp_date=20200613) t GROUP BY f_staff_id) t_v ON t_a.f_staff_id=t_v.f_staff_id) t_merge GROUP BY vendor, f_staff_id, dim";
//
//        String sql = s6;
//
//        System.out.println(sqlTF(sql));

        Set<String> tdwTables = new HashSet<>();
        tdwTables.add("t_ods_consume_kd_basic_detail_h");
        List<String> ddls=tryMakeTdwTableDdls(tdwTables);
        System.out.println("------------------------------------------------------------------");
        for(String s:ddls){
            System.out.println(s);
        }
    }
}
