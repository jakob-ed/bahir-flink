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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.emulator.PinotHelper;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EmulatedPinotBatchSinkTest extends PinotUnitTestBase {
    private static final TableConfig TABLE_CONFIG = PinotTableConfig.getTableConfig();
    private static final String TABLE_NAME = TABLE_CONFIG.getTableName();
    private static final Schema TABLE_SCHEMA = PinotTableConfig.getTableSchema();
    private static final PinotHelper pinotHelper = getPinotHelper();

    @BeforeEach
    public void beforeEach() throws Exception {
        // PinotEmulatorManager.launchDocker();
        pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
        pinotHelper.createTable(TABLE_CONFIG, TABLE_SCHEMA);
        // TODO: docker run --network=pinot-demo --name pinot-quickstart -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
    }

    @AfterEach
    public void afterEach() throws Exception {
        // pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    @Test
    public void testFlinkSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        List<SingleColumnTableRow> input =
                Stream.of(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten", "Eleven", "Twelve")
                        .map(SingleColumnTableRow::new)
                        .collect(Collectors.toList());

        // Create test stream
        DataStream<SingleColumnTableRow> theData =
                env.fromCollection(input)
                        .name("Test input");

        SegmentNameGenerator segmentNameGenerator = new PinotSegmentNameGenerator(TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter("flink-pinot-connector-test");

        // Sink into Pinot
        theData.sinkTo(new PinotSink<>(getPinotControllerHost(), getPinotControllerPort(), TABLE_NAME, 5, segmentNameGenerator, fsAdapter))
                .name("Pinot sink");

        // Run
        env.execute();

        TimeUnit.MILLISECONDS.sleep(500);

        // Now get the result from Pinot and verify if everything is there
        ResultSet resultSet = pinotHelper.getTableEntries(TABLE_NAME, 15);

        assertEquals("Wrong number of elements", input.size(), resultSet.getRowCount());

        // Check output strings
        List<String> output = IntStream.range(0, resultSet.getRowCount())
                .mapToObj(i -> resultSet.getString(i, 0))
                .collect(Collectors.toList());

        for (SingleColumnTableRow test : input) {
            assertTrue("Missing " + test.getCol1(), output.contains(test.getCol1()));
        }
    }

    static class SingleColumnTableRow {

        private String _col1;

        SingleColumnTableRow(@JsonProperty(value = "col1", required = true) String col1) {
            this._col1 = col1;
        }

        @JsonProperty("col1")
        public String getCol1() {
            return this._col1;
        }

        public void setCol1(String _col1) {
            this._col1 = _col1;
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
