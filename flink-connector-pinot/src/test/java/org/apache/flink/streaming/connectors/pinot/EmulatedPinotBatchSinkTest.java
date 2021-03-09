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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.PinotSinkSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * E2e tests for Pinot Sink using BATCH execution mode
 */
public class EmulatedPinotBatchSinkTest extends PinotTestBase {
    private static final TableConfig TABLE_CONFIG = PinotTableConfig.getTableConfig();
    private static final String TABLE_NAME = TABLE_CONFIG.getTableName();
    private static final Schema TABLE_SCHEMA = PinotTableConfig.getTableSchema();

    /**
     * Create an empty test table before each test.
     *
     * @throws Exception
     */
    @BeforeEach
    public void beforeEach() throws Exception {
        pinotHelper.createTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Delete the test table after each test.
     *
     * @throws Exception
     */
    @AfterEach
    public void afterEach() throws Exception {
        pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Tests the BATCH execution of the {@link PinotSink}.
     *
     * @throws Exception
     */
    @Test
    public void testBatchSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        List<SingleColumnTableRow> input =
                Stream.of(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten", "Eleven", "Twelve")
                        .map(col1 -> new SingleColumnTableRow(col1, System.currentTimeMillis()))
                        .collect(Collectors.toList());

        // Create test stream
        DataStream<SingleColumnTableRow> theData =
                env.fromCollection(input)
                        .name("Test input");

        PinotSinkSegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter();
        JsonSerializer<SingleColumnTableRow> jsonSerializer = new SingleColumnTableRowSerializer();

        EventTimeExtractor<SingleColumnTableRow> eventTimeExtractor = new SingleColumnTableRowEventTimeExtractor();

        // Sink into Pinot
        theData.sinkTo(new PinotSink<>(getPinotHost(), getPinotControllerPort(), TABLE_NAME, 5, "flink-pinot-connector-test", jsonSerializer, eventTimeExtractor, segmentNameGenerator, fsAdapter))
                .name("Pinot sink");

        // Run
        env.execute();

        TimeUnit.MILLISECONDS.sleep(500);

        // Now get the result from Pinot and verify if everything is there
        ResultSet resultSet = pinotHelper.getTableEntries(TABLE_NAME, 15);

        Assertions.assertEquals(input.size(), resultSet.getRowCount(), "Wrong number of elements");

        // Check output strings
        List<String> output = IntStream.range(0, resultSet.getRowCount())
                .mapToObj(i -> resultSet.getString(i, 0))
                .collect(Collectors.toList());

        for (SingleColumnTableRow test : input) {
            Assertions.assertTrue(output.contains(test.getCol1()), "Missing " + test.getCol1());
        }
    }
}
