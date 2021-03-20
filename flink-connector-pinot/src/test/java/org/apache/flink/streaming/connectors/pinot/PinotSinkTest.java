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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.PinotSinkSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.flink.util.Preconditions;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * E2e tests for Pinot Sink using BATCH and STREAMING execution mode
 */
public class PinotSinkTest extends PinotTestBase {

    private static final int MAX_ROWS_PER_SEGMENT = 5;
    private static final long STREAMING_CHECKPOINTING_INTERVAL = 50;
    private static final int DATA_CHECKING_TIMEOUT_SECONDS = 60;
    private static final AtomicBoolean hasFailedOnce = new AtomicBoolean(false);
    private static CountDownLatch latch;

    @BeforeEach
    public void setUp() throws IOException {
        super.setUp();
        // Reset hasFailedOnce flag used during failure recovery testing before each test.
        hasFailedOnce.set(false);
        // Reset latch used to keep the generator streaming source up until the test is completed.
        latch = new CountDownLatch(1);
    }

    /**
     * Tests the BATCH execution of the {@link PinotSink}.
     *
     * @throws Exception
     */
    @Test
    public void testBatchSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(2);

        List<String> rawData = getRawTestData(12);
        DataStream<SingleColumnTableRow> dataStream = setupBatchDataSource(env, rawData);
        setupSink(dataStream);

        // Run
        env.execute();

        // Check for data in Pinot
        checkForDataInPinotWithRetry(rawData);
    }

    /**
     * Tests failure recovery of the {@link PinotSink} using BATCH execution mode.
     *
     * @throws Exception
     */
    @Test
    public void testFailureRecoveryInBatchingSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10));
        env.setParallelism(2);

        List<String> rawData = getRawTestData(12);
        DataStream<SingleColumnTableRow> dataStream = setupBatchDataSource(env, rawData);
        dataStream = setupFailingMapper(dataStream, 8);
        setupSink(dataStream);

        // Run
        env.execute();

        // Check for data in Pinot
        checkForDataInPinotWithRetry(rawData);
    }

    /**
     * Tests the STREAMING execution of the {@link PinotSink}.
     *
     * @throws Exception
     */
    @Test
    public void testStreamingSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(2);
        env.enableCheckpointing(STREAMING_CHECKPOINTING_INTERVAL);

        List<String> rawData = getRawTestData(20);
        DataStream<SingleColumnTableRow> dataStream = setupStreamingDataSource(env, rawData);
        setupSink(dataStream);

        // Start execution of job
        env.executeAsync();

        // Check for data in Pinot
        checkForDataInPinotWithRetry(rawData);

        // Generator source can now shut down
        latch.countDown();
    }

    /**
     * Tests failure recovery of the {@link PinotSink} using STREAMING execution mode.
     *
     * @throws Exception
     */
    @Test
    public void testFailureRecoveryInStreamingSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(STREAMING_CHECKPOINTING_INTERVAL);

        List<String> rawData = getRawTestData(20);
        DataStream<SingleColumnTableRow> dataStream = setupStreamingDataSource(env, rawData);
        // With a segment size of MAX_ROWS_PER_SEGMENT = 5 elements and a parallelism of 2,
        // the failure will be raised once the first 2 segments were committed to Pinot.
        dataStream = setupFailingMapper(dataStream, 12);
        setupSink(dataStream);

        // Start execution of job
        env.executeAsync();

        // Check for data in Pinot
        checkForDataInPinotWithRetry(rawData);

        // Generator source can now shut down
        latch.countDown();
    }

    /**
     * Generates a small test dataset consisting of {@link SingleColumnTableRow}s.
     *
     * @return List of SingleColumnTableRow
     */
    private List<String> getRawTestData(int numItems) {
        return IntStream.range(1, numItems + 1)
                .mapToObj(num -> "ColValue" + num)
                .collect(Collectors.toList());
    }

    /**
     * Setup the data source for STREAMING tests.
     *
     * @param env           Stream execution environment
     * @param rawDataValues Data values to send
     * @return resulting data stream
     */
    private DataStream<SingleColumnTableRow> setupStreamingDataSource(StreamExecutionEnvironment env, List<String> rawDataValues) {
        SimpleStreamingSource source = new SimpleStreamingSource(rawDataValues, 10);
        return env.addSource(source)
                .name("Test input");
    }

    /**
     * Setup the data source for BATCH tests.
     *
     * @param env           Stream execution environment
     * @param rawDataValues Data values to send
     * @return resulting data stream
     */
    private DataStream<SingleColumnTableRow> setupBatchDataSource(StreamExecutionEnvironment env, List<String> rawDataValues) {
        return env.fromCollection(rawDataValues)
                .map(value -> new SingleColumnTableRow(value, System.currentTimeMillis()))
                .name("Test input");
    }

    /**
     * Setup a mapper that fails when processing the nth element with n = failOnceAtNthElement.
     *
     * @param dataStream           Input data stream
     * @param failOnceAtNthElement Number of elements to process before raising the exception
     * @return resulting data stream
     */
    private DataStream<SingleColumnTableRow> setupFailingMapper(DataStream<SingleColumnTableRow> dataStream, int failOnceAtNthElement) {
        AtomicInteger messageCounter = new AtomicInteger(0);

        return dataStream.map(element -> {
            if (!hasFailedOnce.get() && messageCounter.incrementAndGet() == failOnceAtNthElement) {
                hasFailedOnce.set(true);
                // Wait more than STREAMING_CHECKPOINTING_INTERVAL to ensure
                // that at least one checkpoint was created before raising the exception.
                Thread.sleep(4 * STREAMING_CHECKPOINTING_INTERVAL);
                throw new Exception(String.format("Mapper was expected to fail after %d elements", failOnceAtNthElement));
            }
            return element;
        });
    }

    /**
     * Sets up a DataStream using the provided execution environment and the provided input data.
     *
     * @param dataStream data stream
     */
    private void setupSink(DataStream<SingleColumnTableRow> dataStream) {
        String tempDirPrefix = "flink-pinot-connector-test";
        PinotSinkSegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(getTableName(), "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter(tempDirPrefix);
        JsonSerializer<SingleColumnTableRow> jsonSerializer = new SingleColumnTableRowSerializer();

        EventTimeExtractor<SingleColumnTableRow> eventTimeExtractor = new SingleColumnTableRowEventTimeExtractor();

        PinotSink<SingleColumnTableRow> sink = new PinotSink.Builder<SingleColumnTableRow>(getPinotHost(), getPinotControllerPort(), getTableName())
                .withMaxRowsPerSegment(MAX_ROWS_PER_SEGMENT)
                .withTempDirectoryPrefix(tempDirPrefix)
                .withJsonSerializer(jsonSerializer)
                .withEventTimeExtractor(eventTimeExtractor)
                .withSegmentNameGenerator(segmentNameGenerator)
                .withFileSystemAdapter(fsAdapter)
                .withNumCommitThreads(2)
                .build();

        // Sink into Pinot
        dataStream.sinkTo(sink).name("Pinot sink");
    }

    /**
     * As Pinot might take some time to index the recently pushed segments we might need to retry
     * the {@link #checkForDataInPinot} method multiple times. This method provides a simple wrapper
     * using linear retry backoff delay.
     *
     * @param rawData Data to expect in the Pinot table
     * @throws InterruptedException
     */
    private void checkForDataInPinotWithRetry(List<String> rawData) throws InterruptedException, PinotControllerApiException {
        long endTime = System.currentTimeMillis() + 1000L * DATA_CHECKING_TIMEOUT_SECONDS;
        // Use max 10 retries with linear retry backoff delay
        long retryDelay = 1000L / 10 * DATA_CHECKING_TIMEOUT_SECONDS;
        while (System.currentTimeMillis() < endTime) {
            try {
                checkForDataInPinot(rawData);
                // In case of no error, we can skip further retries
                return;
            } catch (AssertionFailedError | PinotControllerApiException | PinotClientException e) {
                // In case of an error retry after delay
                Thread.sleep(retryDelay);
            }
        }

        // Finally check for data in Pinot if retryTimeoutInSeconds was exceeded
        checkForDataInPinot(rawData);
    }

    /**
     * Checks whether data is present in the Pinot target table. numElementsToCheck defines the
     * number of elements (from the head of data) to check for existence in the pinot table.
     *
     * @param rawData Data to expect in the Pinot table
     * @throws AssertionFailedError        in case the assertion fails
     * @throws PinotControllerApiException in case there aren't any rows in the Pinot table
     */
    private void checkForDataInPinot(List<String> rawData) throws AssertionFailedError, PinotControllerApiException, PinotClientException {
        // Now get the result from Pinot and verify if everything is there
        ResultSet resultSet = pinotHelper.getTableEntries(getTableName(), rawData.size() + 5);

        Assertions.assertEquals(rawData.size(), resultSet.getRowCount(),
                String.format("Expected %d elements in Pinot but saw %d", rawData.size(), resultSet.getRowCount()));

        // Check output strings
        List<String> output = IntStream.range(0, resultSet.getRowCount())
                .mapToObj(i -> resultSet.getString(i, 0))
                .collect(Collectors.toList());

        for (String test : rawData) {
            Assertions.assertTrue(output.contains(test), "Missing " + test);
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

    /**
     * Simple source that publishes data and finally waits for {@link #latch}.
     * Adapted from: https://github.com/apache/flink/blob/9ee91efa94c67f797b39a967af9671a1df3381b4/flink-end-to-end-tests/flink-file-sink-test/src/main/java/FileSinkProgram.java
     */
    private static class SimpleStreamingSource implements SourceFunction<SingleColumnTableRow>, CheckpointedFunction {

        private static final int serialVersionUID = 1;

        private final List<String> rawDataValues;
        private final int sleepDurationMs;

        private int numElementsEmitted = 0;

        private ListState<Integer> state = null;

        SimpleStreamingSource(final List<String> rawDataValues, final int sleepDurationMs) {
            this.rawDataValues = rawDataValues;
            Preconditions.checkArgument(sleepDurationMs > 0);
            this.sleepDurationMs = sleepDurationMs;
        }

        @Override
        public void run(final SourceContext<SingleColumnTableRow> ctx) throws InterruptedException {
            while (numElementsEmitted < rawDataValues.size()) {
                synchronized (ctx.getCheckpointLock()) {
                    SingleColumnTableRow element = new SingleColumnTableRow(
                            rawDataValues.get(numElementsEmitted), System.currentTimeMillis());
                    ctx.collect(element);
                    numElementsEmitted++;
                }
                Thread.sleep(sleepDurationMs);
            }

            // Keep generator source up until the test was completed.
            latch.await();
        }

        @Override
        public void cancel() {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("state", IntSerializer.INSTANCE));

            for (Integer i : state.get()) {
                numElementsEmitted += i;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(numElementsEmitted);
        }
    }
}
