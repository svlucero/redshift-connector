package com.connect.redshift;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.StringUtils;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TestRedshiftDatabaseDialect {

    @org.junit.Test
    public void test() {
        Map<String, String> c = new HashMap<String, String>();
        ConfigDef def = new ConfigDef();
        TableId tableId = new TableId(null, null, "myTable");
        ColumnId columnPK1 = new ColumnId(tableId, "id1");
        ColumnId columnPK2 = new ColumnId(tableId, "id2");
        ColumnId columnA = new ColumnId(tableId, "columnA");
        ColumnId columnB = new ColumnId(tableId, "columnB");
        ColumnId columnC = new ColumnId(tableId, "columnC");
        ColumnId columnD = new ColumnId(tableId, "columnD");
        List pkColumns = Arrays.asList(columnPK1, columnPK2);
        List columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);

        RedshiftDatabaseDialect dialect = newSinkDialectFor(Collections.singleton("TABLE"));
        assertEquals(
                "BEGIN; DELETE FROM  \"myTable\" WHERE \"id1\" = ? AND \"id2\" = ? ; INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\",\"columnC\",\"columnD\") VALUES(?,?,?,?,?,?); COMMIT;",
                dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD));

    }


    protected RedshiftDatabaseDialect newSinkDialectFor(Set<String> tableTypes) {
        Map<String, String> connProps = new HashMap<String, String>();
        connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:redshift:test");
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, StringUtils.join(tableTypes, ","));
        JdbcSinkConfig sinkConfig = new JdbcSinkConfig(connProps);
        RedshiftDatabaseDialect dialect = new RedshiftDatabaseDialect(sinkConfig);
        return dialect;
    }
}