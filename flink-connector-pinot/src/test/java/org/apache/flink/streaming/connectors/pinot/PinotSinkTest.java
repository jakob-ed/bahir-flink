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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.PinotSinkSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.client.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * E2e tests for Pinot Sink using BATCH and STREAMING execution mode
 */
public class PinotSinkTest extends PinotTestBase {

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
        executeOnMiniCluster(env.getStreamGraph().getJobGraph());

        checkForDataInPinotWithRetry(data, 20);
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

        List<SingleColumnTableRow> data = getTestData(12);
        this.setupDataStream(env, data);

        // Run
        executeOnMiniCluster(env.getStreamGraph().getJobGraph());

        // We only expect the first 100 elements to be already committed to Pinot.
        // The remaining would follow once we increase the input data size.
        // The stream executions stops once the last input tuple was sent to the sink.
        checkForDataInPinotWithRetry(data, 20);
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
     * Executes a given JobGraph on a MiniCluster.
     *
     * @param jobGraph JobGraph to execute
     * @throws Exception
     */
    private void executeOnMiniCluster(JobGraph jobGraph) throws Exception {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
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

        String tempDirPrefix = "flink-pinot-connector-test";
        PinotSinkSegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter(tempDirPrefix);
        JsonSerializer<SingleColumnTableRow> jsonSerializer = new SingleColumnTableRowSerializer();

        EventTimeExtractor<SingleColumnTableRow> eventTimeExtractor = new SingleColumnTableRowEventTimeExtractor();

        PinotSink<SingleColumnTableRow> sink = new PinotSink.Builder<SingleColumnTableRow>(getPinotHost(), getPinotControllerPort(), TABLE_NAME)
                .withMaxRowsPerSegment(5)
                .withTempDirectoryPrefix(tempDirPrefix)
                .withJsonSerializer(jsonSerializer)
                .withEventTimeExtractor(eventTimeExtractor)
                .withSegmentNameGenerator(segmentNameGenerator)
                .withFileSystemAdapter(fsAdapter)
                .withNumCommitThreads(2)
                .build();

        // Sink into Pinot
        theData.sinkTo(sink).name("Pinot sink");
    }

    /**
     * As Pinot might take some time to index the recently pushed segments we might need to retry
     * the {@link #checkForDataInPinot} method multiple times. This method provides a simple wrapper
     * using linear retry backoff delay.
     *
     * @param data                  Data to expect in the Pinot table
     * @param retryTimeoutInSeconds Maximum duration in seconds to wait for the data to arrive
     * @throws InterruptedException
     */
    private void checkForDataInPinotWithRetry(List<SingleColumnTableRow> data, int retryTimeoutInSeconds) throws InterruptedException {
        long endTime = System.currentTimeMillis() + 1000L * retryTimeoutInSeconds;
        // Use max 10 retries with linear retry backoff delay
        long retryDelay = 1000L / 10 * retryTimeoutInSeconds;
        do {
            try {
                checkForDataInPinot(data);
                // In case of no error, we can skip further retries
                return;
            } catch (AssertionFailedError | PinotControllerApiException e) {
                // In case of an error retry after delay
                Thread.sleep(retryDelay);
            }
        } while (System.currentTimeMillis() < endTime);
    }

    /**
     * Checks whether data is present in the Pinot target table. numElementsToCheck defines the
     * number of elements (from the head of data) to check for existence in the pinot table.
     *
     * @param data Data to expect in the Pinot table
     * @throws AssertionFailedError        in case the assertion fails
     * @throws PinotControllerApiException in case there aren't any rows in the Pinot table
     */
    private void checkForDataInPinot(List<SingleColumnTableRow> data) throws AssertionFailedError, PinotControllerApiException {
        // Now get the result from Pinot and verify if everything is there
        ResultSet resultSet = pinotHelper.getTableEntries(TABLE_NAME, data.size() + 5);

        // Check output strings
        List<String> output = IntStream.range(0, resultSet.getRowCount())
                .mapToObj(i -> resultSet.getString(i, 0))
                .collect(Collectors.toList());

        List<SingleColumnTableRow> dataToCheck = data.stream().limit(100).collect(Collectors.toList());
        for (SingleColumnTableRow test : dataToCheck) {
            Assertions.assertTrue(output.contains(test.getCol1()), "Missing " + test.getCol1());
        }
    }

    /**
     * EventTimeExtractor for {@link SingleColumnTableRow} used in e2e tests.
     * Extracts the timestamp column from {@link SingleColumnTableRow}.
     */
    private static class SingleColumnTableRowEventTimeExtractor implements EventTimeExtractor<SingleColumnTableRow> {

        @Override
        public long getEventTime(SingleColumnTableRow element, SinkWriter.Context context) {
            return element.getTimestamp();
        }

        @Override
        public String getTimeColumn() {
            return "timestamp";
        }

        @Override
        public TimeUnit getSegmentTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }
    }
}
