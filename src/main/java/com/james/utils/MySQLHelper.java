package com.james.utils;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class MySQLHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLHelper.class);

    /**
     * 返回一条记录 1.默认的情况下要保证表的字段和javabean的属性一致（字符一致即可，对大小写不敏感）
     *
     * @param t
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static <T> T getSingle(Class<?> t, String sql, Object[] params) throws Exception {
        T entity = null;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            entity = (T) qr.query(sql, new BeanHandler<T>((Class<T>) t), params);
            return entity;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }

    /**
     * 返回一条记录 1.默认的情况下要保证表的字段和javabean的属性一致（字符一致即可，对大小写不敏感）
     *
     * @param t
     * @param sql
     * @param params
     * @param propertiesMap Map<tableField,EntityField>
     * @return
     * @throws Exception
     */
    public static <T> T getSingleByProcessor(Class<?> t, String sql, Object[] params, Map<String, String> propertiesMap) throws Exception {
        T entity = null;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        // 用构建好的HashMap建立一个BeanProcessor对象
        BeanProcessor bean = new BeanProcessor(propertiesMap);
        RowProcessor processor = new BasicRowProcessor(bean);
        try {
            entity = (T) qr.query(sql, new BeanHandler<T>((Class<T>) t, processor), params);
            return entity;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }

    /**
     * 返回多条记录
     *
     * @param t
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static <T> List<T> getMultipleList(Class<?> t, String sql, Object[] params) throws Exception {
        List<T> entityList = null;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            entityList = qr.query(sql, new BeanListHandler<T>((Class<T>) t), params);
            return entityList;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }

    /**
     * 返回多条记录
     *
     * @param t
     * @param sql
     * @param params
     * @param propertiesMap Map<tableField,EntityField>
     * @return
     * @throws Exception
     */
    public static <T> List<T> getMultipleList(Class<?> t, String sql, Object[] params, Map<String, String> propertiesMap) throws Exception {
        List<T> entityList = null;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        BeanProcessor bean = new BeanProcessor(propertiesMap);
        RowProcessor processor = new BasicRowProcessor(bean);
        try {
            entityList = qr.query(sql, new BeanListHandler<T>((Class<T>) t, processor), params);
            return entityList;
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }

    /**
     * 返回唯一值
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static Object getScalar(String sql, Object[] params) throws Exception {
        Object ret = null;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            ret = qr.query(sql, new ScalarHandler<Integer>(), params);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
        return ret;
    }

    /**
     * 插入一条记录,返回自增长id
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static int insertSingle(String sql, Object[] params) throws Exception {
        int ret = 0;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            // qr.inser
            int count = qr.update(sql, params);
            BigInteger newId = (BigInteger) qr.query("select last_insert_id()", new ScalarHandler<BigInteger>(1));
            ret = Integer.valueOf(String.valueOf(newId));
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
        return ret;
    }

    /**
     * 插入多条记录
     *
     * @param sql
     * @param params
     * @throws Exception
     */
    public static void insertMultipleList(String sql, Object[][] params) throws Exception {
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            qr.batch(sql, params);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }

    /**
     * 删除记录
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static int delete(String sql, Object[] params) throws Exception {
        int count = 0;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            count = qr.update(sql, params);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
        return count;
    }

    /**
     * 更新记录
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static <T> int update(String sql, Object[] params) throws Exception {
        int count = 0;
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            count = qr.update(sql, params);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }

        return count;
    }

    /**
     * 更新多条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public static void updateMultipleList(String sql, Object[][] params) throws Exception {
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource());
        try {
            qr.batch(sql, params);
        } catch (Exception e) {
            LOGGER.error(e.toString());
            throw e;
        }
    }
}
