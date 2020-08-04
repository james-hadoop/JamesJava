package com.james._demo;

import com.james.utils.HiveLineageLogEntity;
import com.james.utils.HiveLineageLogEntityDao;

import java.util.Date;


/**
 * Created by James on 20-7-25 下午10:54
 */
public class MysqlHelperDemo {
    public static void main(String[] args) throws Exception {
        HiveLineageLogEntity entity = HiveLineageLogEntityDao.getById(2);
        System.out.println(entity);

        entity = new HiveLineageLogEntity("hello james");
        HiveLineageLogEntityDao.add(entity);
    }
}
