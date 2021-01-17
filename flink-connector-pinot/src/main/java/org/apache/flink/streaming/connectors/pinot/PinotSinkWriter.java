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

    private final Integer subtaskId;
    private Integer latestCommittedId = -1;

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;

    private List<IN> rows;

    public PinotSinkWriter(Integer subtaskId, String pinotControllerHost, String pinotControllerPort, String tableName) throws IOException {
        this.subtaskId = checkNotNull(subtaskId);
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);

        this.rows = new ArrayList<>();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        // TODO: we need to move this to disk, to prevent a large memory footprint
        this.rows.add(element);
    }

    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean b) throws IOException {
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);
        Schema tableSchema = controllerApi.getSchema(this.tableName);
        TableConfig tableConfig = controllerApi.getTableConfig(this.tableName);

        String commitHash = UUID.randomUUID().toString();
        String pathPrefix = Helper.getPathPrefix(this, this.subtaskId, commitHash);
        LOG.info("Using path '{}' for storing committables", pathPrefix);

        // Stores row items in JSON format on disk
        File dataFile = new File(pathPrefix + "data.json");
        if (new File(pathPrefix).mkdirs()) {
            LOG.info("Successfully created directories for {} [subtaskId={}]", dataFile.toPath(), this.subtaskId);
        }
        Helper.writeToFile(dataFile, this.rows);

        // Generate and store segment
        File segmentFile = new File(pathPrefix + "segment");
        Helper.generateSegment(dataFile, segmentFile, FileFormat.JSON, null, tableConfig, tableSchema, true);

        // Submit path to segment as committable
        PinotSinkCommittable committable = new PinotSinkCommittable(segmentFile.getPath());
        return Collections.singletonList(committable);
    }

    public void initializeState(List<PinotWriterState> states) {
        this.latestCommittedId = states.stream()
                .mapToInt(state -> state.latestCommittedSegmentId)
                .max().orElseGet(() -> -1);
    }

    @Override
    public List<PinotWriterState> snapshotState() throws IOException {
        PinotWriterState state = new PinotWriterState(this.latestCommittedId);
        return Collections.singletonList(state);
    }

    @Override
    public void close() throws InterruptedException {
        // TODO
    }

    static class Helper {
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
        public static void generateSegment(File dataFile, File outDir, FileFormat _format, RecordReaderConfig recordReaderConfig, TableConfig tableConfig, Schema schema, Boolean _postCreationVerification) {
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
            segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
            segmentGeneratorConfig.setFormat(_format);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
            segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
            segmentGeneratorConfig.setTableName(tableConfig.getTableName());

            try {
                SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig);
                driver.build();
                String segmentName = driver.getSegmentName();
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
