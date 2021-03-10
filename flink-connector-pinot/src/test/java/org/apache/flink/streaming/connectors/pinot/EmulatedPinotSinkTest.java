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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.PinotSinkSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.client.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * E2e tests for Pinot Sink using BATCH and STREAMING execution mode
 */
public class EmulatedPinotSinkTest extends PinotTestBase {

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

        List<SingleColumnTableRow> data = getTestData(12);
        this.setupDataStream(env, data);

        // Run
        env.execute();

        TimeUnit.MILLISECONDS.sleep(500);

        checkForDataInPinot(data, data.size());
    }

    /**
     * Tests the STREAMING execution of the {@link PinotSink}.
     *
     * @throws Exception
     */
    @Test
    public void testStreamingSink() throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        env.enableCheckpointing(50);

        List<SingleColumnTableRow> data = getTestData(1000);
        this.setupDataStream(env, data);

        // Run
        env.execute();

        // Wait until the checkpoint was created and the segments were committed by the GlobalCommitter
        TimeUnit.SECONDS.sleep(5);

        // We only expect the first 100 elements to be already committed to Pinot.
        // The remaining would follow once we increase the input data size.
        // The stream executions seems to stop once the last input tuple was sent to the sink
        checkForDataInPinot(data, 100);
    }

    /**
     * Generates a small test dataset consisting of {@link SingleColumnTableRow}s.
     *
     * @return List of SingleColumnTableRow
     */
    private List<SingleColumnTableRow> getTestData(int numItems) {
        return IntStream.range(1, numItems + 1)
                .mapToObj(num -> "ColValue" + num)
                .map(col1 -> new SingleColumnTableRow(col1, System.currentTimeMillis()))
                .collect(Collectors.toList());
    }

    /**
     * Sets up a DataStream using the provided execution environment and the provided input data.
     *
     * @param env  stream execution environment
     * @param data Input data
     */
    private void setupDataStream(StreamExecutionEnvironment env, List<SingleColumnTableRow> data) {
        // Create test stream
        DataStream<SingleColumnTableRow> theData =
                env.fromCollection(data)
                        .name("Test input");

        PinotSinkSegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter();
        JsonSerializer<SingleColumnTableRow> jsonSerializer = new SingleColumnTableRowSerializer();

        EventTimeExtractor<SingleColumnTableRow> eventTimeExtractor = new SingleColumnTableRowEventTimeExtractor();

        // Sink into Pinot
        theData.sinkTo(new PinotSink<>(getPinotHost(), getPinotControllerPort(), TABLE_NAME, 5, "flink-pinot-connector-test", jsonSerializer, eventTimeExtractor, segmentNameGenerator, fsAdapter))
                .name("Pinot sink");
    }

    /**
     * Checks whether data is present in the Pinot target table
     *
     * @param data Data to expect in the Pinot table
     * @throws Exception
     */
    private void checkForDataInPinot(List<SingleColumnTableRow> data, int numElementsToCheck) throws Exception {
        // Now get the result from Pinot and verify if everything is there
        ResultSet resultSet = pinotHelper.getTableEntries(TABLE_NAME, data.size() + 5);

        if (numElementsToCheck == data.size()) {
            // Check for exact number
            Assertions.assertEquals(numElementsToCheck, resultSet.getRowCount(), "Wrong number of elements");
        } else if (numElementsToCheck < data.size()) {
            // Check if at least numElementsToCheck elements are present
            Assertions.assertTrue(resultSet.getRowCount() >= numElementsToCheck, "To few elements");
        } else {
            throw new IllegalArgumentException("numElementsToCheck must not be larger than size of input data");
        }

        // Check output strings
        List<String> output = IntStream.range(0, resultSet.getRowCount())
                .mapToObj(i -> resultSet.getString(i, 0))
                .collect(Collectors.toList());

        List<SingleColumnTableRow> dataToCheck = data.stream().limit(100).collect(Collectors.toList());
        for (SingleColumnTableRow test : dataToCheck) {
            Assertions.assertTrue(output.contains(test.getCol1()), "Missing " + test.getCol1());
        }
    }
}
