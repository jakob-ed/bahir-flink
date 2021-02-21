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
import org.apache.flink.streaming.connectors.pinot.PinotSinkUtils;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

public class PinotWriterSegment<IN> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PinotWriterSegment.class);

    private final int maxRowsPerSegment;
    private final FileSystemAdapter fsAdapter;

    private List<IN> rows;
    private Long minTimestamp = null;
    private Long maxTimestamp = null;

    protected PinotWriterSegment(int maxRowsPerSegment, FileSystemAdapter fsAdapter) {
        checkArgument(maxRowsPerSegment > 0L);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.fsAdapter = checkNotNull(fsAdapter);
        this.rows = new ArrayList<>();
    }

    public void write(IN element, Long timestmap) {
        if (!this.acceptsElements()) {
            throw new IllegalStateException("This PinotSegmentWriter does not accept any elements anymore.");
        }
        this.rows.add(element);
        this.minTimestamp = PinotSinkUtils.getMin(this.minTimestamp, timestmap);
        this.maxTimestamp = PinotSinkUtils.getMax(this.maxTimestamp, timestmap);
    }

    public PinotSinkCommittable prepareCommit() throws IOException {
        File dataFile = this.writeToFile();
        return new PinotSinkCommittable(dataFile, this.minTimestamp, this.maxTimestamp);
    }

    private File writeToFile() throws IOException {
        // Create folder in temp directory for storing data
        Path dir = this.fsAdapter.createTempDirectory();
        LOG.info("Using path '{}' for storing committables", dir.toAbsolutePath());

        // Stores row items in JSON format on disk
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

        String FILE_NAME = "data.json";
        File dataFile = this.fsAdapter.writeToFile(dir, FILE_NAME, json);
        LOG.info("Successfully written data to file {} in directory {}", FILE_NAME, dir.getFileName());

        return dataFile;
    }

    public boolean acceptsElements() {
        return this.rows.size() < this.maxRowsPerSegment;
    }
}
