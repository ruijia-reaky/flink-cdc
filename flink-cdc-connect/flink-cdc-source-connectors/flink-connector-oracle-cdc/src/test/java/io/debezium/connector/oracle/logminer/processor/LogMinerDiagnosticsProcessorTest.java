/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogMinerDiagnosticsProcessorTest {
    private final PreparedStatement statement = mock(PreparedStatement.class);
    private final ResultSet results = mock(ResultSet.class);
    private final OracleOffsetContext offset = mock(OracleOffsetContext.class);

    private MemoryLogMinerEventProcessor processor(boolean enabled, boolean startTransaction)
            throws Exception {
        OracleConnectorConfig config =
                new OracleConnectorConfig(
                        Configuration.create()
                                .with("topic.prefix", "diagnostics-test")
                                .with("database.dbname", "TEST")
                                .with("log.mining.diagnostics.oldest.enabled", enabled)
                                .with("log.mining.diagnostics.timing.enabled", enabled)
                                .build());
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true);
        when(offset.getScn()).thenReturn(Scn.ONE);
        CommitScn commits = mock(CommitScn.class);
        when(commits.getMaxCommittedScn()).thenReturn(Scn.valueOf(100));
        when(offset.getCommitScn()).thenReturn(commits);
        when(statement.executeQuery()).thenReturn(results);
        return new MemoryLogMinerEventProcessor(
                context,
                config,
                null,
                mock(EventDispatcher.class),
                null,
                offset,
                mock(OracleDatabaseSchema.class),
                mock(LogMinerStreamingChangeEventSourceMetrics.class)) {
            @Override
            protected PreparedStatement createQueryStatement() {
                return statement;
            }

            @Override
            protected void processResults(OraclePartition partition, ResultSet resultSet)
                    throws SQLException, InterruptedException {
                if (!startTransaction) {
                    super.processResults(partition, resultSet);
                    return;
                }
                LogMinerEventRow row = mock(LogMinerEventRow.class);
                when(row.getTransactionId()).thenReturn("a1");
                when(row.getScn()).thenReturn(Scn.valueOf(10));
                when(row.getChangeTime()).thenReturn(Instant.EPOCH);
                when(row.getThread()).thenReturn(2);
                handleStart(row);
                counters.rows++;
            }
        };
    }

    @Test
    void emptyBatchPreservesOffsetAndClosesCursor() throws Exception {
        assertThat(processor(true, false).process(Scn.ONE, Scn.valueOf(100))).isEqualTo(Scn.ONE);
        verify(results).next(); // Final false is still read through the instrumented method.
        verify(results).close();
        verify(statement).close();
        verify(offset, never()).setScn(any());
    }

    @Test
    void nextFailurePropagatesOriginalException() throws Exception {
        MemoryLogMinerEventProcessor p = processor(true, false);
        SQLException failure = new SQLException("ORA-01013: cancelled");
        when(results.next()).thenThrow(failure);
        assertThatThrownBy(() -> p.process(Scn.ONE, Scn.valueOf(100))).isSameAs(failure);
        verify(results).close();
        verify(offset, never()).setScn(any());
    }

    @Test
    void executeFailurePropagatesOriginalException() throws Exception {
        MemoryLogMinerEventProcessor p = processor(true, false);
        SQLException failure = new SQLException("ORA-01291: missing log");
        when(statement.executeQuery()).thenThrow(failure);
        assertThatThrownBy(() -> p.process(Scn.ONE, Scn.valueOf(100))).isSameAs(failure);
        verify(statement).close();
    }

    @Test
    void diagnosticsKeepMiningAndRecoveryOffsetsSeparate() throws Exception {
        assertThat(processor(true, true).process(Scn.ONE, Scn.valueOf(100)))
                .isEqualTo(Scn.valueOf(100));
        verify(offset).setScn(Scn.valueOf(9));
    }
}
