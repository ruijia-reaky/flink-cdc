/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.*;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/** Regression cases for LOB statement boundaries and immutable rollback journals. */
class LobRollbackResolverTest {
    private static final String EMPTY = "AAAAAAAAAAAAAAAAAA";
    private static final String ROW = "AAASj9AAMAAAACFAAC";
    private static final TableId TABLE = new TableId("XE", "APP", "T");

    private static LogMinerEvent event(EventType type, String row, String rs) {
        return new LogMinerEvent(type, Scn.valueOf(10), TABLE, row, rs, Instant.EPOCH);
    }

    private static LogMinerEvent undo(EventType type) {
        LogMinerEventRow row =
                new LogMinerEventRow() {
                    @Override
                    public EventType getEventType() {
                        return type;
                    }

                    @Override
                    public String getRowId() {
                        return ROW;
                    }

                    @Override
                    public TableId getTableId() {
                        return TABLE;
                    }

                    @Override
                    public Scn getScn() {
                        return Scn.valueOf(11);
                    }
                };
        return new RollbackToSavepointEvent(row);
    }

    @Test
    void removesInsertAndItsLobSupplement() {
        assertThat(
                        LobRollbackResolver.resolve(
                                Arrays.asList(
                                        event(EventType.INSERT, EMPTY, "a"),
                                        event(EventType.UPDATE, ROW, "b"),
                                        undo(EventType.DELETE))))
                .isEmpty();
    }

    @Test
    void preservesInsertBeforeSavepoint() {
        LogMinerEvent insert = event(EventType.INSERT, EMPTY, "a");
        LogMinerEvent supplement = event(EventType.UPDATE, ROW, "b");
        assertThat(
                        LobRollbackResolver.resolve(
                                Arrays.asList(
                                        insert,
                                        supplement,
                                        event(EventType.UPDATE, ROW, "c"),
                                        undo(EventType.UPDATE))))
                .containsExactly(insert, supplement);
    }

    @Test
    void internalBoundaryPreservesEmptyLobInsert() {
        LogMinerEvent insert = event(EventType.INSERT, EMPTY, "a");
        assertThat(
                        LobRollbackResolver.resolve(
                                Arrays.asList(
                                        insert,
                                        event(EventType.INTERNAL, ROW, "b"),
                                        event(EventType.UPDATE, ROW, "c"),
                                        undo(EventType.UPDATE))))
                .containsExactly(insert);
    }

    @Test
    void removesOutOfLineStatementAsAGroup() {
        assertThat(
                        LobRollbackResolver.resolve(
                                Arrays.asList(
                                        event(EventType.UPDATE, EMPTY, "a"),
                                        event(EventType.SELECT_LOB_LOCATOR, EMPTY, "a"),
                                        event(EventType.LOB_WRITE, EMPTY, "b"),
                                        event(EventType.INTERNAL, ROW, "c"),
                                        undo(EventType.UPDATE))))
                .isEmpty();
    }

    @Test
    void repeatedResolutionDoesNotApplyUndoTwice() {
        LogMinerEvent original = event(EventType.INSERT, ROW, "a");
        List<LogMinerEvent> journal =
                Arrays.asList(original, event(EventType.UPDATE, ROW, "b"), undo(EventType.UPDATE));
        assertThat(LobRollbackResolver.resolve(journal)).containsExactly(original);
        assertThat(LobRollbackResolver.resolve(journal)).containsExactly(original);
        assertThat(journal).hasSize(3);
    }

    @Test
    void resolvesMultipleSavepointRollbacksInOrder() {
        LogMinerEvent original = event(EventType.INSERT, ROW, "a");
        assertThat(
                        LobRollbackResolver.resolve(
                                Arrays.asList(
                                        original,
                                        event(EventType.UPDATE, ROW, "b"),
                                        undo(EventType.UPDATE),
                                        event(EventType.UPDATE, ROW, "c"),
                                        undo(EventType.UPDATE))))
                .containsExactly(original);
    }

    @Test
    void refusesAmbiguousUndoInsteadOfLeakingData() {
        assertThatThrownBy(() -> LobRollbackResolver.resolve(Arrays.asList(undo(EventType.UPDATE))))
                .isInstanceOf(IllegalStateException.class);
    }
}
