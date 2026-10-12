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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Collector;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Collections;

import static io.debezium.config.CommonConnectorConfig.TOPIC_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.reader.MySqlRecordEmitter}. */
public class MySqlRecordEmitterTest {

    @Test
    public void testHeartbeatEventHandling() throws Exception {
        Configuration dezConf =
                JdbcConfiguration.create()
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 100)
                        .with(TOPIC_PREFIX, "mysql_binlog_source")
                        .build();

        MySqlConnectorConfig mySqlConfig = new MySqlConnectorConfig(dezConf);
        // Construct a plain HeartbeatImpl directly. Debezium 2.0's HeartbeatFactory would route to
        // a DatabaseHeartbeatImpl (which needs a live JDBC connection) because MySqlConnectorConfig
        // reports a non-null heartbeat action query; this unit test only needs a heartbeat record.
        Heartbeat heartbeat =
                new HeartbeatImpl(
                        mySqlConfig.getHeartbeatInterval(),
                        mySqlConfig
                                .getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY)
                                .heartbeatTopic(),
                        mySqlConfig.getLogicalName(),
                        SchemaNameAdjuster.create());
        BinlogOffset fakeOffset = BinlogOffset.ofBinlogFilePosition("fake-file", 15213L);
        MySqlRecordEmitter<Void> recordEmitter = createRecordEmitter();
        MySqlBinlogSplitState splitState = createBinlogSplitState();
        heartbeat.forcedBeat(
                Collections.emptyMap(),
                fakeOffset.getOffset(),
                record -> {
                    try {
                        recordEmitter.emitRecord(
                                SourceRecords.fromSingleRecord(record),
                                new TestingReaderOutput<>(),
                                splitState);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to emit heartbeat record", e);
                    }
                });
        heartbeat.close();
        assertNotNull(splitState.getStartingOffset());
        assertEquals(0, splitState.getStartingOffset().compareTo(fakeOffset));
    }

    private MySqlRecordEmitter<Void> createRecordEmitter() {
        return new MySqlRecordEmitter<>(
                new DebeziumDeserializationSchema<Void>() {
                    @Override
                    public void deserialize(SourceRecord record, Collector<Void> out) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public TypeInformation<Void> getProducedType() {
                        return TypeInformation.of(Void.class);
                    }
                },
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                false);
    }

    private MySqlBinlogSplitState createBinlogSplitState() {
        return new MySqlBinlogSplitState(
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofEarliest(),
                        BinlogOffset.ofNonStopping(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0));
    }
}
