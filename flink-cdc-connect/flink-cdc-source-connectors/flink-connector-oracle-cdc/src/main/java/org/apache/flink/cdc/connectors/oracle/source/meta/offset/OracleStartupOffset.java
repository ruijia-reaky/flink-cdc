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

package org.apache.flink.cdc.connectors.oracle.source.meta.offset;

import org.apache.flink.table.api.ValidationException;

import io.debezium.connector.oracle.CommitScn;

import java.util.Map;

/** Validates user supplied initial offsets, without changing checkpoint offsets. */
public final class OracleStartupOffset {
    private OracleStartupOffset() {}

    public static void validate(Map<String, String> offset) {
        if (offset == null || offset.get("scn") == null || !offset.get("scn").matches("[0-9]+")) {
            throw new ValidationException(
                    "Oracle specific-offset requires a non-negative decimal scn");
        }
        for (String key : offset.keySet()) {
            if (!"scn".equals(key) && !"commit_scn".equals(key) && !"lcr_position".equals(key)) {
                throw new ValidationException("Unknown Oracle specific-offset key: " + key);
            }
        }
        String commit = offset.get("commit_scn");
        if (commit != null) {
            try {
                if (commit.isEmpty()) {
                    throw new IllegalArgumentException("empty commit_scn");
                }
                CommitScn.valueOf(commit);
            } catch (RuntimeException e) {
                throw new ValidationException("Invalid Oracle specific-offset commit_scn", e);
            }
        }
    }
}
