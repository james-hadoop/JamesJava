package com.james.hive.lineage.tools;

import java.util.Set;

/**
 * Created by James on 20-7-23 下午11:50
 */
public abstract class AbstractSqlProcessor implements ISqlProcessor {
    private ISqlReader sqlReader;
    private ISqlWriter1Tuple sqlWriter;

    public AbstractSqlProcessor(ISqlReader sqlReader, ISqlWriter1Tuple sqlWriter) {
        this.sqlReader = sqlReader;
        this.sqlWriter = sqlWriter;
    }

    int doWorkOnSql() {
        Set<String> sqlSet = sqlReader.readSql();

        Set<String> processedSqlSet = processSql(sqlSet);

        int sqlCnt = sqlWriter.writeSql(processedSqlSet);
        return sqlCnt;
    }
}
