package com.james.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by James on 20-7-25 下午11:13
 */
public class HiveLineageLogEntityDao {
    private static Logger LOG = LoggerFactory.getLogger(HiveLineageLogEntityDao.class);

    private static Map<String, String> propertiesMap = new HashMap<String, String>();
    private static final String SQL_SELECT = "SELECT id, lineage_str, lineage_str_sha, update_time FROM txkd_dc_hive_lineage_log where id=?";
    private static final String SQL_ADD = "INSERT INTO txkd_dc_hive_lineage_log(lineage_str,lineage_str_sha,update_time) VALUES(?,?,?)";


    static {
        init();
    }

    private static void init() {
        Map<String, String> propertiesMap = new HashMap<String, String>();
        propertiesMap.put("id", "id");
        propertiesMap.put("lineage_str", "lineageStr");
        propertiesMap.put("lineage_str_sha", "lineageStrSha");
        propertiesMap.put("update_time", "updateTime");
    }

    public static HiveLineageLogEntity getById(int id) throws Exception {
        HiveLineageLogEntity ret = null;

        try {
            Object[] params = {id};
            ret = MySQLHelper.getSingleByProcessor(HiveLineageLogEntity.class, SQL_SELECT, params, propertiesMap);

        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }

        return ret;
    }

    public static int add(HiveLineageLogEntity entity) throws Exception {
        int ret = 0;
        try {
            Object[] params = {entity.getLineageStr(), entity.geLineageStrSha(), entity.getUpdateTime()};
            ret = MySQLHelper.insertSingle(SQL_ADD, params);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        }

        return ret;
    }
}
