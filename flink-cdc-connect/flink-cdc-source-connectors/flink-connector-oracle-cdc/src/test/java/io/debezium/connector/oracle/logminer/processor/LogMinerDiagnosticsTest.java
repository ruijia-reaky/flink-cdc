/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryTransaction;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogMinerDiagnosticsTest {
    private final AtomicLong clock = new AtomicLong();
    private final List<String> output = new ArrayList<>();

    private LogMinerDiagnostics create(boolean oldest, boolean timing) {
        return new LogMinerDiagnostics(
                Configuration.create()
                        .with("log.mining.diagnostics.oldest.enabled", oldest)
                        .with("log.mining.diagnostics.timing.enabled", timing)
                        .with("log.mining.diagnostics.interval.ms", 1000)
                        .build(),
                clock::get,
                output::add);
    }

    private void finish(LogMinerDiagnostics diagnostics, long offset) {
        diagnostics.finish(
                true,
                Scn.valueOf(offset),
                "threads",
                Scn.valueOf(50),
                "MEMORY",
                false,
                () -> "oldest=none");
    }

    @Test
    void disabledDoesNotReadClockOrTransactions() {
        LogMinerDiagnostics d =
                new LogMinerDiagnostics(
                        Configuration.create().build(),
                        () -> {
                            throw new AssertionError("clock read while disabled");
                        },
                        output::add);
        d.begin(Scn.ONE, Scn.valueOf(50));
        d.end(LogMinerDiagnostics.Stage.RESULT_SET_NEXT, d.start());
        d.row(EventType.START);
        d.finish(
                true,
                Scn.ONE,
                null,
                Scn.ONE,
                "MEMORY",
                false,
                () -> {
                    throw new AssertionError("cache read while disabled");
                });
        assertThat(output).isEmpty();
    }

    @Test
    void rateLimitsAndReportsOnlyLatestBatchAndResetsFreezeOnProgress() {
        LogMinerDiagnostics d = create(true, true);
        d.begin(Scn.ONE, Scn.valueOf(50));
        d.row(EventType.START);
        finish(d, 1);
        clock.set(500_000_000);
        d.begin(Scn.ONE, Scn.valueOf(50));
        finish(d, 1);
        assertThat(output).hasSize(2);
        clock.set(1_000_000_000);
        d.begin(Scn.ONE, Scn.valueOf(50));
        finish(d, 1);
        assertThat(output.get(2)).contains("operations={}");
        assertThat(output.get(3)).contains("observedOffsetUnchangedMs=1000");
        clock.set(2_000_000_000);
        d.begin(Scn.ONE, Scn.valueOf(50));
        finish(d, 2);
        assertThat(output.get(5)).contains("observedOffsetUnchangedMs=0");
    }

    @Test
    void accumulatesCallsAndMaximumUsingMonotonicClock() {
        LogMinerDiagnostics d = create(false, true);
        d.begin(Scn.ONE, Scn.valueOf(50));
        long started = d.start();
        clock.addAndGet(3_000_000);
        d.end(LogMinerDiagnostics.Stage.RESULT_SET_NEXT, started);
        started = d.start();
        clock.addAndGet(7_000_000);
        d.end(LogMinerDiagnostics.Stage.RESULT_SET_NEXT, started);
        d.row(EventType.COMMIT);
        finish(d, 1);
        assertThat(output).hasSize(1);
        assertThat(output.get(0)).contains("RESULT_SET_NEXTMs=10/calls=2/maxMs=7", "COMMIT=1");
    }

    @Test
    void oldestOnlyDoesNotCollectRowTiming() {
        LogMinerDiagnostics d = create(true, false);
        d.begin(Scn.ONE, Scn.valueOf(50));
        finish(d, 1);
        assertThat(output).hasSize(1);
        assertThat(output.get(0)).startsWith("LogMinerOldestTransaction");
    }

    @Test
    void describesEmptyAndPopulatedTransactionsWithoutChangingThem() {
        MemoryTransaction tx = new MemoryTransaction("aa", Scn.valueOf(123), Instant.EPOCH, "u", 2);
        assertThat(LogMinerDiagnostics.describe(Optional.of(tx), 0, 3))
                .contains(
                        "oldestXid=aa",
                        "oldestCachedScn=123",
                        "cachedEventCount=0",
                        "redoThread=2",
                        "transactionTime=1970-01-01T00:00:00Z");
        assertThat(tx.getEvents()).isEmpty();
        assertThat(LogMinerDiagnostics.describe(Optional.empty(), 0, 0)).contains("oldest=none");
    }

    @Test
    void rejectsUnboundedLogFrequency() {
        assertThatThrownBy(
                        () ->
                                new LogMinerDiagnostics(
                                        Configuration.create()
                                                .with("log.mining.diagnostics.interval.ms", 0)
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
