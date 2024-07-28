package com.connect.redshift;

import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Collection;

public class RedshiftDatabaseDialect extends GenericDatabaseDialect {
    public RedshiftDatabaseDialect(AbstractConfig config) {
        super(config);
    }

    @Override
    public String buildUpsertQueryStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("BEGIN;");
        builder.append(" DELETE FROM  ");
        builder.append(table);
        builder.append(" WHERE ");
        builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        builder.append(" ; ");
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        builder.append(";");
        builder.append(" COMMIT;");
        return builder.toString();
    }
}
