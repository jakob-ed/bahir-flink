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

package org.apache.flink.streaming.connectors.pinot.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link PinotWriterSegment} represents exactly one segment that can be found in the Pinot
 * cluster once the commit has been completed.
 *
 * @param <IN> Type of incoming elements
 */
public class PinotWriterSegment<IN> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger("PinotWriterSegment");

    private final int maxRowsPerSegment;
    private final String tempDirPrefix;
    private final FileSystemAdapter fsAdapter;

    private boolean acceptsElements = true;

    private List<IN> elements;
    private File dataFile;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;

    /**
     * @param maxRowsPerSegment Maximum number of rows to be stored within a Pinot segment
     * @param tempDirPrefix     Prefix for temp directories used
     * @param fsAdapter         Filesystem adapter used to save files for sharing files across nodes
     */
    protected PinotWriterSegment(int maxRowsPerSegment, String tempDirPrefix, FileSystemAdapter fsAdapter) {
        checkArgument(maxRowsPerSegment > 0L);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.elements = new ArrayList<>();
    }

    /**
     * Takes elements and stores them in memory until either {@link #maxRowsPerSegment} is reached
     * or {@link #prepareCommit} is called.
     *
     * @param element   Object from upstream task
     * @param timestamp Timestamp assigned to element
     * @throws IOException
     */
    public void write(IN element, long timestamp) throws IOException {
        if (!this.acceptsElements()) {
            throw new IllegalStateException("This PinotSegmentWriter does not accept any elements anymore.");
        }
        this.elements.add(element);
        this.minTimestamp = Long.min(this.minTimestamp, timestamp);
        this.maxTimestamp = Long.max(this.maxTimestamp, timestamp);

        // Writes elements to local filesystem once the maximum number of items is reached
        if (this.elements.size() == this.maxRowsPerSegment) {
            acceptsElements = false;
            dataFile = this.writeToLocalFile();
            this.elements.clear();
        }
    }

    /**
     * Writes elements to local file (if not already done). Copies just created file to the shared
     * filesystem defined via {@link FileSystemAdapter} and creates a {@link PinotSinkCommittable}.
     *
     * @return {@link PinotSinkCommittable} pointing to file on shared filesystem
     * @throws IOException
     */
    public PinotSinkCommittable prepareCommit() throws IOException {
        if (dataFile == null) {
            dataFile = this.writeToLocalFile();
        }
        String path = fsAdapter.copyToSharedFileSystem(dataFile);
        return new PinotSinkCommittable(path, this.minTimestamp, this.maxTimestamp);
    }

    /**
     * Takes elements from {@link #elements} and writes them to the local filesystem in JSON format.
     *
     * @return File containing the just written data
     * @throws IOException
     */
    private File writeToLocalFile() throws IOException {
        // Create folder in temp directory for storing data
        Path dir = Files.createTempDirectory(tempDirPrefix);
        LOG.info("Using path '{}' for storing committables", dir.toAbsolutePath());

        // Stores row items in JSON format on disk
        List<String> json = this.elements.stream()
                .map(b -> {
                    // TODO: serializer
                    try {
                        JsonNode jsonNode = JsonUtils.objectToJsonNode(b);
                        return jsonNode;
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage());
                    }
                })
                .map(JsonNode::toString)
                .collect(Collectors.toList());

        String FILE_NAME = "data.json";

        File dataFile = new File(dir.toAbsolutePath() + "/" + dir);
        Files.write(dataFile.toPath(), json, Charset.defaultCharset());
        LOG.info("Successfully written data to file {} in directory {}", FILE_NAME, dir.getFileName());

        return dataFile;
    }

    /**
     * Determines whether this segment can accept at least one more elements
     *
     * @return True if at least one more element will be accepted
     */
    public boolean acceptsElements() {
        return acceptsElements;
    }
}
