/*
 * Copyright Debezium Authors.
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class UnscaledNumberCompatibilityTest {
    private OracleValueConverters converters(boolean enabled) {
        OracleConnectorConfig config =
                new OracleConnectorConfig(
                        Configuration.create()
                                .with("topic.prefix", "test")
                                .with("database.dbname", "TEST")
                                .with("number.unscaled.as.string", enabled)
                                .build());
        return new OracleValueConverters(config, mock(OracleConnection.class));
    }

    @Test
    void unscaledValuesPreservePlainPrecisionAndNull() {
        OracleValueConverters c = converters(true);
        Column column = Column.editor().name("N").jdbcType(Types.NUMERIC).optional(true).create();
        Schema schema = c.schemaBuilder(column).optional().build();
        Field field = new Field("N", 0, schema);
        assertThat(schema.type()).isEqualTo(Schema.Type.STRING);
        for (String value :
                new String[] {"123456789012345678901234567890.0123", "-0.00001", "1E+20", "0"}) {
            assertThat(c.converter(column, field).convert(new BigDecimal(value)))
                    .isEqualTo(new BigDecimal(value).toPlainString());
        }
        assertThat(c.converter(column, field).convert(null)).isNull();
    }

    @Test
    void explicitScaleRetainsIntegerAndDecimalSchemas() {
        OracleValueConverters c = converters(true);
        Column integer =
                Column.editor().name("I").jdbcType(Types.NUMERIC).length(9).scale(0).create();
        Column decimal =
                Column.editor().name("D").jdbcType(Types.NUMERIC).length(20).scale(4).create();
        assertThat(c.schemaBuilder(integer).build().type()).isEqualTo(Schema.Type.INT32);
        assertThat(c.schemaBuilder(decimal).build().name())
                .isEqualTo("org.apache.kafka.connect.data.Decimal");
    }

    @Test
    void optOutRetainsUpstreamVariableScale() {
        Column column = Column.editor().name("N").jdbcType(Types.NUMERIC).create();
        assertThat(converters(false).schemaBuilder(column).build().name())
                .isEqualTo("io.debezium.data.VariableScaleDecimal");
    }
}
