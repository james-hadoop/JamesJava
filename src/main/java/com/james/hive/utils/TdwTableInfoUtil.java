package com.james.hive.utils;

import com.google.gson.Gson;
import com.james.hive.utils.entity.ColsInfo;
import com.james.hive.utils.entity.RetObj;
import com.james.hive.utils.entity.TableInfo;
import com.james.hive.utils.entity.TdwTableInfo;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

public class TdwTableInfoUtil {
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

    public static String makeTdwTableDDL(String tdwDB, String tdwTable) throws Exception {
        if (null == tdwDB || null == tdwTable) {
            return null;
        }

        String dbPrefix = "create database if not exists " + tdwDB + "; use " + tdwDB + ";";
        String dllPrefix = "CREATE TABLE if not exists " + tdwTable + "( ";
        String ddlPostfix = " );";

        ResponseEntity<String> queryResult = reqTdw(tdwDB, tdwTable);

        String httpBodyTmp = queryResult.getBody();
        String httpBody = new String(httpBodyTmp.getBytes("ISO-8859-1"), "UTF-8");
        System.out.println(httpBody);
        System.out.println("-----------------------------------------------------------------------------------");

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

        System.out.println(String.format("%s", ddl));
        return ddl;
    }


    public static void main(String[] args) throws Exception {
        String tdwDB = "pcg_txkd_shared_data_app";
        String tdwTable = "t_dwt_consume_video_rowkeyperformance_normal_d";

        makeTdwTableDDL(tdwDB, tdwTable);
//        System.out.println(tdwTableInfo.getRetObj());

    }
}
