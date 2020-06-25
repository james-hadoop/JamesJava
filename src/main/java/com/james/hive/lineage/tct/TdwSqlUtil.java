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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TdwSqlUtil {
    private static String[] tdwDBArr = {"pcg_txkd_shared_data_app", "sng_mp_etldata", "sng_mediaaccount_app", "pcg_txkd_qb_info_app", "omg_mobile_newsdev", "omg_tdbank", "hlw"};

    private static String[] pattern;
    private static String[] replace;

    /**
     * 初始化，用数组来存储被替换和替换的字符串
     */
    private static void init() {
        pattern = new String[9];
        replace = new String[9];

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
        replace[6] = "";

        pattern[7] = "distribute\\s+by\\s+.+";
        replace[7] = ";";

        pattern[8] = "  ";
        replace[8] = " ";
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

        String dbPrefix = "create database if not exists " + tdwDB + "; use " + tdwDB + ";";
        String dllPrefix = "CREATE TABLE if not exists " + tdwTable + "( ";
        String ddlPostfix = " );";

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

//        System.out.println(String.format("%s", ddl));
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
                //            System.out.println(tdwDBArr[i]);
                try {
                    tdwTableDDL = makeTdwTableDdl(tdwDBArr[i], tdwTable);
                    if (null != tdwTableDDL) {
                        tdwTableDdlList.add(tdwTableDDL);
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("!!! " + tdwTable + " -> " + e.getMessage());
                }
            }
        }

        return tdwTableDdlList;
    }

    public static void main(String[] args) {

        String sql = "";

        System.out.println(sqlTF(sql));

    }
}
