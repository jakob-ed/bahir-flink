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

package org.apache.flink.streaming.connectors.pinot.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.connectors.pinot.PinotControllerApi;
import org.apache.pinot.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@CommandLine.Command(name = "MetricsCollector", mixinStandardHelpOptions = true,
        description = "Start the metrics logger.")
public class MetricsCollector implements Callable<Integer> {

    static final String METRICS_CSV_DIRECTORY = "/collected-metrics/";

    private static final Logger LOG = LoggerFactory.getLogger("MetricsCollector");

    @CommandLine.Option(names = "--numTuples", required = true, description = "The overall number of tuples to expect.")
    private Integer numTuples;

    @CommandLine.Option(names = "--segmentSize", required = true,
            description = "The number of tuples per segment.")
    private Integer segmentSize;

    @CommandLine.Option(names = "--checkpointingInterval", required = true,
            description = "The Flink checkpointing interval.")
    private Integer checkpointingInterval;

    @CommandLine.Option(names = "--delay", required = true, description = "Startup delay.")
    private Long delay;

    private File csvOutputFile;

    private void writeSegmentMetricsToCsv(Map<String, Metrics> collectedSegments) throws IOException {
        if (csvOutputFile.exists()) {
            csvOutputFile.delete();
        }
        csvOutputFile.createNewFile();
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println("segmentName,segmentStartTime,segmentEndTime,segmentCreationTime,segmentPushTime,segmentTotalDocs");
            collectedSegments.values().stream()
                    .map(Metrics::toCsvRow)
                    .forEach(pw::println);
        }
        LOG.info("Successfully written collected metrics to {}", csvOutputFile.getAbsolutePath());
    }

    private void fetchAndWriteEventTimes(String benchmarkId) throws Exception {
        String broker = FlinkApp.PINOT_CONTROLLER_HOST + ":8000";
        Connection pinotConnection = ConnectionFactory.fromHostList(broker);
        String query = String.format("SELECT eventTime, flinkSourceTime FROM %s LIMIT %d", FlinkApp.PinotTableConfig.TABLE_NAME, numTuples);

        LOG.info("Now fetching event times from broker {} ...", broker);
        Request pinotClientRequest = new Request("sql", query);
        ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotClientRequest);

        if (pinotResultSetGroup.getResultSetCount() != 1) {
            throw new Exception("Could not find any data in Pinot cluster.");
        }

        File eventCsvFile = new File(METRICS_CSV_DIRECTORY + benchmarkId + "__event-times.csv");
        LOG.info("Now writing event times to {} ...", eventCsvFile.getAbsolutePath());
        try (PrintWriter pw = new PrintWriter(eventCsvFile)) {
            pw.println("eventTime,flinkSinkTime");
            for (int i = 0; i < pinotResultSetGroup.getResultSetCount(); i++) {
                ResultSet resultSet = pinotResultSetGroup.getResultSet(i);
                IntStream.range(0, resultSet.getRowCount())
                        .mapToObj(rowIdx -> resultSet.getString(rowIdx, 0) + "," + resultSet.getString(rowIdx, 1))
                        .forEach(pw::println);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error while writing event times to {}. {}", eventCsvFile.getAbsolutePath(), e.getMessage());
        }
        LOG.info("Successfully written event times to {}.", eventCsvFile.getAbsolutePath());
    }

    @Override
    public Integer call() throws Exception {
        LOG.info("Startup delay of {} ms. Sleeping now...", delay);
        Thread.sleep(delay);

        String benchmarkId = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd__HH-mm-ss"));
        csvOutputFile = new File(METRICS_CSV_DIRECTORY + benchmarkId + "__metrics.csv");
        LOG.info("Using file {} to write collected metrics to", csvOutputFile.getAbsolutePath());

        PinotControllerApi pinotControllerApi = new PinotControllerApi(FlinkApp.PINOT_CONTROLLER_HOST, FlinkApp.PINOT_CONTROLLER_PORT);
        final String tableName = FlinkApp.PinotTableConfig.TABLE_NAME;
        int numSegmentsToExpect = (int) Math.ceil((double) this.numTuples / this.segmentSize);
        Map<String, Metrics> collectedSegments = new HashMap<>();

        while (collectedSegments.size() < numSegmentsToExpect) {
            List<String> discoveredSegmentNames = pinotControllerApi.getSegmentNames(tableName);
            LOG.info("Discovered {} segments: {}", discoveredSegmentNames.size(), discoveredSegmentNames);

            for (final String segmentName : discoveredSegmentNames) {
                if (collectedSegments.containsKey(segmentName)) {
                    continue;
                }
                JsonNode metadataJson = pinotControllerApi.getSegmentMetadata(tableName, segmentName);
                collectedSegments.put(segmentName, Metrics.fromJsonNode(metadataJson));
            }

            this.writeSegmentMetricsToCsv(collectedSegments);
            Thread.sleep(this.checkpointingInterval);
        }

        this.writeSegmentMetricsToCsv(collectedSegments);
        this.fetchAndWriteEventTimes(benchmarkId);

        return 0;
    }
}

class Metrics {

    final String segmentName;
    final long segmentStartTime;
    final long segmentEndTime;
    final long segmentCreationTime;
    final long segmentPushTime;

    final int segmentTotalDocs;

    public Metrics(String segmentName, long segmentStartTime, long segmentEndTime, long segmentCreationTime, long segmentPushTime, int segmentTotalDocs) {
        this.segmentName = segmentName;
        this.segmentStartTime = segmentStartTime;
        this.segmentEndTime = segmentEndTime;
        this.segmentCreationTime = segmentCreationTime;
        this.segmentPushTime = segmentPushTime;
        this.segmentTotalDocs = segmentTotalDocs;
    }

    String toCsvRow() {
        String arrayString = Stream.of(Arrays.asList(segmentName, segmentStartTime, segmentEndTime, segmentCreationTime, segmentPushTime, segmentTotalDocs))
                .map(Objects::toString)
                .collect(Collectors.joining(","));
        return arrayString.substring(1, arrayString.length() - 1);
    }

    static Metrics fromJsonNode(JsonNode json) {
        String segmentName = json.get("segment.name").asText();
        long segmentStartTime = Long.parseLong(json.get("segment.start.time").asText());
        long segmentEndTime = Long.parseLong(json.get("segment.end.time").asText());
        long segmentCreationTime = Long.parseLong(json.get("segment.creation.time").asText());
        long segmentPushTime = Long.parseLong(json.get("segment.offline.push.time").asText());

        int segmentTotalDocs = Integer.parseInt(json.get("segment.total.docs").asText());

        return new Metrics(segmentName, segmentStartTime, segmentEndTime, segmentCreationTime, segmentPushTime, segmentTotalDocs);
    }
}