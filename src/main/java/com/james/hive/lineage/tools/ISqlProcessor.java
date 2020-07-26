package com.james.hive.lineage.tools;

import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:49
 */
public interface ISqlProcessor {
    Set<String> processSql(Set<String> sqlSet);
}
