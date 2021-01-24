/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, PinotWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkWriter.class);

    private final int subtaskId;
    private int latestSegmentId = -1;

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final Integer rowsPerSegment;

    private List<IN> rows;

    public PinotSinkWriter(int subtaskId, String pinotControllerHost, String pinotControllerPort, String tableName, int rowsPerSegment) throws IOException {
        this.subtaskId = checkNotNull(subtaskId);
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);
        this.rowsPerSegment = checkNotNull(rowsPerSegment);

        this.rows = new ArrayList<>();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        // TODO: we need to move this to disk, to prevent a large memory footprint
        this.rows.add(element);

        LOG.info("Added element to local storage. Size is now {} [subtaskId={}]", this.rows.size(), this.subtaskId);

        if (this.rows.size() >= this.rowsPerSegment) {
            this.latestSegmentId = Helper.commitSegment(this);
            LOG.info("Reset local cache [subtaskId={}]", this.subtaskId);
            this.rows = new ArrayList<>();
        }
    }

    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean b) throws IOException {
        // TODO
        return new ArrayList<>();
    }

    public void initializeState(List<PinotWriterState> states) {
        this.latestSegmentId = states.stream()
                .filter(state -> state.latestCommittedSegmentId == this.subtaskId)
                .mapToInt(state -> state.latestCommittedSegmentId)
                .max().orElseGet(() -> -1);
    }

    @Override
    public List<PinotWriterState> snapshotState() throws IOException {
        PinotWriterState state = new PinotWriterState(this.latestSegmentId);
        return Collections.singletonList(state);
    }

    @Override
    public void close() throws InterruptedException {
        // TODO
    }

    static class Helper {

        public static <IN> Integer commitSegment(PinotSinkWriter<IN> instance) throws IOException {
            PinotControllerApi controllerApi = new PinotControllerApi(instance.pinotControllerHost, instance.pinotControllerPort);
            Schema tableSchema = controllerApi.getSchema(instance.tableName);
            TableConfig tableConfig = controllerApi.getTableConfig(instance.tableName);

            String commitHash = UUID.randomUUID().toString();
            String pathPrefix = Helper.getPathPrefix(instance, instance.subtaskId, commitHash);
            LOG.info("Using path '{}' for storing committables", pathPrefix);

            // Stores row items in JSON format on disk
            File dataFile = new File(pathPrefix + "data.json");
            if (new File(pathPrefix).mkdirs()) {
                LOG.info("Successfully created directories for {} [subtaskId={}]", dataFile.toPath(), instance.subtaskId);
            }
            Helper.writeToFile(dataFile, instance.rows);

            // Generate and store segment
            File segmentFile = new File(pathPrefix + "segment");
            Integer segmentId = instance.latestSegmentId + 1;
            String segmentName = String.format("%s_%d-%d", instance.tableName, instance.subtaskId, segmentId);
            Helper.generateSegment(segmentName, dataFile, segmentFile, FileFormat.JSON, null, tableConfig, tableSchema, true);


            try {
                UploadSegmentCommand cmd = new UploadSegmentCommand();
                cmd.setControllerHost(instance.pinotControllerHost);
                cmd.setControllerPort(instance.pinotControllerPort);
                cmd.setSegmentDir(segmentFile.getAbsolutePath());
                cmd.execute();
            } catch (Exception e) {
                LOG.info("Could not upload segment {}", segmentFile.toPath(), e);
                throw new IOException(e.getMessage());
            }

            LOG.info("Successfully uploaded segment at {}", segmentFile);
            return segmentId;
        }

        /**
         * @param instance
         * @param subtaskId
         * @param commitHash
         * @param <IN>
         * @return
         */
        public static <IN> String getPathPrefix(PinotSinkWriter<IN> instance, Integer subtaskId, String commitHash) {
            // TODO: Retrieve temp dir
            return String.format("/tmp/flink-connector-pinot/sink-%s/subtaskId=%d/%s/", instance.hashCode(), subtaskId, commitHash);
        }

        /**
         * @param outFile
         * @param rows
         * @param <IN>
         * @throws IOException
         */
        public static <IN> void writeToFile(File outFile, List<IN> rows) throws IOException {
            List<String> json = rows.stream()
                    .map(b -> {
                        try {
                            JsonNode jsonNode = JsonUtils.objectToJsonNode(b);
                            return jsonNode;
                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    })
                    .map(JsonNode::toString)
                    .collect(Collectors.toList());
            Files.write(outFile.toPath(), json, Charset.defaultCharset());
        }

        /**
         * This method was adapted from org.apache.pinot.tools.admin.command.CreateSegmentCommand.java
         *
         * @param dataFile
         * @param outDir
         * @param _format
         * @param recordReaderConfig
         * @param tableConfig
         * @param schema
         * @param _postCreationVerification
         */
        public static void generateSegment(String segmentName, File dataFile, File outDir, FileFormat _format, RecordReaderConfig recordReaderConfig, TableConfig tableConfig, Schema schema, Boolean _postCreationVerification) {
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
            segmentGeneratorConfig.setSegmentName(segmentName);
            segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
            segmentGeneratorConfig.setFormat(_format);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
            segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
            segmentGeneratorConfig.setTableName(tableConfig.getTableName());

            try {
                SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig);
                driver.build();
                File indexDir = new File(outDir, segmentName);
                LOG.info("Successfully created segment: {} at directory: {}", segmentName, indexDir);
                if (_postCreationVerification) {
                    LOG.info("Verifying the segment by loading it");
                    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
                    LOG.info("Successfully loaded segment: {} of size: {} bytes", segmentName,
                            segment.getSegmentSizeBytes());
                    segment.destroy();
                }
            } catch (Exception e) {
                throw new RuntimeException("Caught exception while generating segment from file: " + dataFile.getPath(), e);
            }
            LOG.info("Successfully created 1 segment from data file: {}", dataFile);
        }
    }
}
