package com.james.hive.lineage.tools;

import java.util.Set;

/**
 * Created by James on 20-7-23 下午10:59
 */
public interface ISqlWriter1Tuple extends ISqlWriter{
    int writeSql(Set<String> sqlSet);

    @Override
    default void writeSql() {

    }
}
