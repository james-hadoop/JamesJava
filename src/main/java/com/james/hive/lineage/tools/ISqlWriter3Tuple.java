package com.james.hive.lineage.tools;

import java.util.Set;

public interface ISqlWriter3Tuple extends ISqlWriter {
    int writeSql(Set<String> successSqlSet, Set<String> failSqlSet, Set<String> dbTableSet);
}
