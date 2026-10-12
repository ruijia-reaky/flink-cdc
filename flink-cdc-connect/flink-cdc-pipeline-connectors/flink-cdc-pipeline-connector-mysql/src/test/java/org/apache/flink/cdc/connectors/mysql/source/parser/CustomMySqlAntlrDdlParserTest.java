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

package org.apache.flink.cdc.connectors.mysql.source.parser;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CustomMySqlAntlrDdlParserTest {
    @Test
    void parsesCreateAndAlterWithCollation() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser();
        parser.setCurrentDatabase("inventory");
        Tables tables = new Tables();
        parser.parse(
                "CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name VARCHAR(64)) COLLATE=utf8mb4_general_ci",
                tables);
        assertThat(parser.getAndClearParsedEvents())
                .hasSize(1)
                .allMatch(CreateTableEvent.class::isInstance);
        assertThat(tables.forTable(new TableId("inventory", null, "products")).columns())
                .hasSize(2);
        parser.parse("ALTER TABLE products ADD COLUMN price DECIMAL(10,2)", tables);
        java.util.List<org.apache.flink.cdc.common.event.SchemaChangeEvent> events =
                parser.getAndClearParsedEvents();
        assertThat(events).hasSize(1).allMatch(AddColumnEvent.class::isInstance);
        AddColumnEvent added = (AddColumnEvent) events.get(0);
        assertThat(added.getAddedColumns()).hasSize(1);
        assertThat(added.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("price");
        assertThat(added.getAddedColumns().get(0).getAddColumn().getType())
                .isEqualTo(org.apache.flink.cdc.common.types.DataTypes.DECIMAL(10, 2));
    }
}
