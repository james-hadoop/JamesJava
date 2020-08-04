package com.james.hive.lineage.tools;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:54
 */
public class ConcreteSqlProcessor extends AbstractSqlProcessor {
    public ConcreteSqlProcessor(ISqlReader sqlReader, ISqlWriter1Tuple sqlWriter) {
        super(sqlReader, sqlWriter);
    }

    @Override
    public Set<String> processSql(Set<String> sqlSet) {
        Set<String> processedSqlSet = new HashSet<>();

        System.out.println("processSql() start...");
        for (String s : sqlSet) {
            processedSqlSet.add("\tbusiness => " + s);
        }
        System.out.println("processSql() stop...");

        return processedSqlSet;
    }
}
