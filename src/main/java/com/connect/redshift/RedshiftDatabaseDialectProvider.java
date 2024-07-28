package com.connect.redshift;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import org.apache.kafka.common.config.AbstractConfig;

public class RedshiftDatabaseDialectProvider extends DatabaseDialectProvider.FixedScoreProvider {

    protected RedshiftDatabaseDialectProvider(String name, int score) {
        super(RedshiftDatabaseDialect.class.getSimpleName(),
                DatabaseDialectProvider.AVERAGE_MATCHING_SCORE);
    }

    public DatabaseDialect create(AbstractConfig abstractConfig) {
        return new RedshiftDatabaseDialect(abstractConfig);
    }
}
