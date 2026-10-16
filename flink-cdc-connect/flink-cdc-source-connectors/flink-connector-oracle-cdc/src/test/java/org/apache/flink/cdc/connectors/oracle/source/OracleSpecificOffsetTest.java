/*
 * Copyright Debezium Authors.
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.*;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class OracleSpecificOffsetTest {
    @Test
    void immutableMapAndStreamSplitUseOracleScn() {
        Map<String, String> map = new HashMap<>();
        map.put("scn", "16892432184625");
        StartupOptions options = StartupOptions.specificOffset(map);
        map.put("scn", "1");
        assertThat(options.getOffset().get("scn")).isEqualTo("16892432184625");
        assertThatThrownBy(() -> options.getOffset().put("scn", "2"))
                .isInstanceOf(UnsupportedOperationException.class);
        SourceConfig config = mock(SourceConfig.class);
        when(config.getStartupOptions()).thenReturn(options);
        StreamSplitAssigner assigner =
                new StreamSplitAssigner(
                        config, mock(DataSourceDialect.class), new RedoLogOffsetFactory());
        RedoLogOffset offset = (RedoLogOffset) assigner.createStreamSplit().getStartingOffset();
        assertThat(offset.getScn()).isEqualTo("16892432184625");
    }

    @Test
    void restoredAssignedStateDoesNotCreateNewStartupSplit() {
        StreamSplitAssigner assigner =
                new StreamSplitAssigner(
                        mock(SourceConfig.class),
                        new StreamPendingSplitsState(true),
                        mock(DataSourceDialect.class),
                        new RedoLogOffsetFactory());
        assertThat(assigner.getNext()).isEmpty();
    }

    @Test
    void rejectsMissingInvalidAndUnknownOffsets() {
        for (Map<String, String> map :
                Arrays.asList(
                        Collections.<String, String>emptyMap(),
                        Collections.singletonMap("scn", "-1"),
                        Collections.singletonMap("scn", "abc"),
                        new HashMap<String, String>() {
                            {
                                put("scn", "1");
                                put("typo", "2");
                            }
                        })) {
            assertThatThrownBy(() -> OracleStartupOffset.validate(map))
                    .isInstanceOf(org.apache.flink.table.api.ValidationException.class);
        }
    }

    @Test
    void datastreamConfigAcceptsMapAndRejectsFilePosition() {
        OracleSourceConfigFactory factory = new OracleSourceConfigFactory();
        factory.hostname("localhost")
                .username("u")
                .password("p")
                .databaseList("DB")
                .startupOptions(
                        StartupOptions.specificOffset(Collections.singletonMap("scn", "100")));
        assertThat(factory.create(0).getStartupOptions().getOffset()).containsEntry("scn", "100");
        factory.startupOptions(StartupOptions.specificOffset("file", 1));
        assertThatThrownBy(() -> factory.create(0)).hasMessageContaining("scn");
    }
}
