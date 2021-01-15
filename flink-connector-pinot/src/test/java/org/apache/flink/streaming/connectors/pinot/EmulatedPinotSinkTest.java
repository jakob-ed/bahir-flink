/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.connectors.pinot;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.emulator.PinotHelper;
import org.apache.flink.streaming.connectors.pinot.emulator.PinotUnitTestBase;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EmulatedPinotSinkTest extends PinotUnitTestBase {
    private static final TableConfig TABLE_CONFIG = PinotTableConfig.getTableConfig();
    private static final String TABLE_NAME = TABLE_CONFIG.getTableName();
    private static final Schema TABLE_SCHEMA = PinotTableConfig.getTableSchema();
    private static final PinotHelper pinotHelper = getPinotHelper();

    @BeforeEach
    public void beforeEach() throws Exception {
        pinotHelper.createTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    @AfterEach
    public void afterEach() throws Exception {
        pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    @Test
    public void testFlinkSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        List<String> input =
                Arrays.asList(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten");

        // Create test stream
        DataStream<String> theData =
                env.fromCollection(input)
                        .name("Test input")
                        .map((MapFunction<String, String>) StringUtils::reverse);

        PinotSinkConfig<String> config = new PinotSinkConfig<>(getPinotControllerHostPort(), TABLE_NAME, new SimpleStringSchema());

        // Sink into Pinot
        theData.sinkTo(new PinotSink<>(config))
                .name("Pinot sink");

        // Run
        env.execute();

        // Now get the result from Pinot and verify if everything is there
        List<String> entries =
                pinotHelper.getTableEntries(TABLE_NAME, 100);

        assertEquals("Wrong number of elements", input.size(), entries.size());

        // Check output strings
        List<String> output = new ArrayList<>();
        entries.forEach(entry -> output.add(entry));

        for (String test : input) {
            assertTrue("Missing " + test, output.contains(StringUtils.reverse(test)));
        }
    }

    static class PinotTableConfig {

        static final String TABLE_NAME = "FLTable";
        static final String SCHEMA_NAME = "FLTableSchema";

        private static SegmentsValidationAndRetentionConfig getValidationConfig() {
            SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
            validationConfig.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
            validationConfig.setSegmentPushType("APPEND");
            validationConfig.setSchemaName(SCHEMA_NAME);
            validationConfig.setReplication("1");
            return validationConfig;
        }

        private static TenantConfig getTenantConfig() {
            TenantConfig tenantConfig = new TenantConfig("DefaultTenant", "DefaultTenant", null);
            return tenantConfig;
        }

        private static IndexingConfig getIndexingConfig() {
            IndexingConfig indexingConfig = new IndexingConfig();
            return indexingConfig;
        }

        private static TableCustomConfig getCustomConfig() {
            TableCustomConfig customConfig = new TableCustomConfig(null);
            ;
            return customConfig;
        }

        static TableConfig getTableConfig() {
            return new TableConfig(
                    TABLE_NAME,
                    TableType.OFFLINE.name(),
                    getValidationConfig(),
                    getTenantConfig(),
                    getIndexingConfig(),
                    getCustomConfig(),
                    null, null, null, null, null,
                    null, null, null, null
            );
        }

        static Schema getTableSchema() {
            Schema schema = new Schema();
            schema.setSchemaName(SCHEMA_NAME);
            schema.addField(new DimensionFieldSpec("col1", FieldSpec.DataType.STRING, true));
            return schema;
        }
    }
}
