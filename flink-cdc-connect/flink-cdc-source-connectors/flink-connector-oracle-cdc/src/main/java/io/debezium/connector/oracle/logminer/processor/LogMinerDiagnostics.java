/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Opt-in, processing-thread-only diagnostics. Never changes transactions or offsets. */
final class LogMinerDiagnostics {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerDiagnostics.class);
    private static final String PREFIX = "log.mining.diagnostics.";

    enum Stage {
        EXECUTE_QUERY,
        RESULT_SET_NEXT,
        ROW_DECODE,
        ROW_PROCESS,
        DML_PARSE,
        DATA_DISPATCH,
        OFFSET_CALCULATION
    }

    private final boolean oldestEnabled;
    private final boolean timingEnabled;
    private final long intervalNanos;
    private final String id;
    private final LongSupplier clock;
    private final Consumer<String> output;
    private final long[] nanos = new long[Stage.values().length];
    private final long[] calls = new long[Stage.values().length];
    private final long[] maxima = new long[Stage.values().length];
    private final EnumMap<EventType, Long> operations = new EnumMap<>(EventType.class);
    private boolean emitted;
    private long lastEmitted;
    private long batchStart;
    private Scn startScn;
    private Scn endScn;
    private Scn observedOffset;
    private long offsetSince;

    LogMinerDiagnostics(Configuration config) {
        this(config, System::nanoTime, message -> LOGGER.info(message));
    }

    LogMinerDiagnostics(Configuration config, LongSupplier clock, Consumer<String> output) {
        oldestEnabled = config.getBoolean(PREFIX + "oldest.enabled", false);
        timingEnabled = config.getBoolean(PREFIX + "timing.enabled", false);
        long interval = config.getLong(PREFIX + "interval.ms", 60_000L);
        if (interval < 1_000 || interval > 86_400_000) {
            throw new IllegalArgumentException(
                    PREFIX + "interval.ms must be between 1000 and 86400000");
        }
        intervalNanos = interval * 1_000_000;
        String label = config.getString(PREFIX + "id", "oracle-cdc");
        id = label.replaceAll("[^a-zA-Z0-9_.-]", "_");
        if (id.length() > 80) {
            throw new IllegalArgumentException(PREFIX + "id must be at most 80 characters");
        }
        this.clock = clock;
        this.output = output;
    }

    boolean enabled() {
        return oldestEnabled || timingEnabled;
    }

    void begin(Scn start, Scn end) {
        if (!enabled()) {
            return;
        }
        startScn = start;
        endScn = end;
        batchStart = clock.getAsLong();
        Arrays.fill(nanos, 0);
        Arrays.fill(calls, 0);
        Arrays.fill(maxima, 0);
        operations.clear();
    }

    long start() {
        return timingEnabled ? clock.getAsLong() : 0;
    }

    void end(Stage stage, long started) {
        if (timingEnabled) {
            long elapsed = clock.getAsLong() - started;
            int i = stage.ordinal();
            nanos[i] += elapsed;
            calls[i]++;
            maxima[i] = Math.max(maxima[i], elapsed);
        }
    }

    void row(EventType type) {
        if (timingEnabled) {
            operations.merge(type, 1L, Long::sum);
        }
    }

    void finish(
            boolean completed,
            Scn offset,
            Object commits,
            Scn lastProcessed,
            String cacheType,
            boolean lobEnabled,
            Supplier<String> oldest) {
        if (!enabled()) {
            return;
        }
        long now = clock.getAsLong();
        if (!offset.equals(observedOffset)) {
            observedOffset = offset;
            offsetSince = now;
        }
        if (emitted && now - lastEmitted < intervalNanos) {
            return;
        }
        emitted = true;
        lastEmitted = now;
        String common =
                " id="
                        + id
                        + " completed="
                        + completed
                        + " miningStartScn="
                        + startScn
                        + " miningEndScn="
                        + endScn
                        + " lastProcessedScn="
                        + lastProcessed
                        + " recoveryOffsetScn="
                        + offset
                        + " commitScnByThread="
                        + commits
                        + " cacheType="
                        + cacheType
                        + " lobEnabled="
                        + lobEnabled;
        if (timingEnabled) {
            StringBuilder message =
                    new StringBuilder("LogMinerBatchDiagnostics")
                            .append(common)
                            .append(" batchMs=")
                            .append((now - batchStart) / 1_000_000)
                            .append(" operations=")
                            .append(operations);
            for (Stage stage : Stage.values()) {
                int i = stage.ordinal();
                message.append(' ')
                        .append(stage)
                        .append("Ms=")
                        .append(nanos[i] / 1_000_000)
                        .append("/calls=")
                        .append(calls[i])
                        .append("/maxMs=")
                        .append(maxima[i] / 1_000_000);
            }
            output.accept(message.toString());
        }
        if (oldestEnabled) {
            output.accept(
                    "LogMinerOldestTransaction"
                            + common
                            + " observedOffsetUnchangedMs="
                            + (now - offsetSince) / 1_000_000
                            + " "
                            + oldest.get());
        }
    }

    static String describe(
            Optional<? extends AbstractTransaction> oldest, int eventCount, int active) {
        if (!oldest.isPresent()) {
            return "activeTransactions=" + active + " oldest=none";
        }
        AbstractTransaction tx = oldest.get();
        return "activeTransactions="
                + active
                + " oldestXid="
                + tx.getTransactionId()
                + " oldestCachedScn="
                + tx.getStartScn()
                + " transactionTime="
                + tx.getChangeTime()
                + " cachedEventCount="
                + eventCount
                + " redoThread="
                + tx.getRedoThreadId();
    }
}
