/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

/** A rollback journal entry retained until commit to avoid applying undo twice. */
public class RollbackToSavepointEvent extends LogMinerEvent {
    public RollbackToSavepointEvent(LogMinerEventRow row) {
        super(row);
    }
}
