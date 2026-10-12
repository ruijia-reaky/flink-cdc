/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.source.schema;

import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OceanBaseDatabaseSchemaTest {
    @Test
    void createsSchemaWithDebezium27() {
        OceanBaseConnectorConfig config =
                new OceanBaseConnectorConfig("mysql", "UTC", new Properties());
        TableId tableId = new TableId("inventory", null, "products");
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(
                                Column.editor()
                                        .name("id")
                                        .type("INT")
                                        .jdbcType(Types.INTEGER)
                                        .optional(false)
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();
        try (TestSchema schema = new TestSchema(config)) {
            schema.register(table);
            assertThat(schema.schemaFor(tableId)).isNotNull();
            assertThat(schema.schemaFor(tableId).keySchema().field("id")).isNotNull();
            assertThat(schema.schemaFor(tableId).valueSchema().field("id")).isNotNull();
        }
        assertThat(config.getLogicalName()).isEqualTo("oceanbase_cdc_connector");
    }

    private static class TestSchema extends OceanBaseDatabaseSchema {
        TestSchema(OceanBaseConnectorConfig config) {
            super(config, Tables.TableFilter.includeAll(), false);
        }

        void register(Table table) {
            buildAndRegisterSchema(table);
        }
    }
}
