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

package org.apache.flink.streaming.connectors.pinot.writer;

import com.google.common.collect.Iterables;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accepts incoming elements and creates {@link PinotSinkCommittable}s out of them on request.
 *
 * @param <IN> Type of incoming elements
 */
public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, Void> {

    private static final Logger LOG = LoggerFactory.getLogger("PinotSinkWriter");

    private final int maxRowsPerSegment;
    private EventTimeExtractor<IN> eventTimeExtractor;
    private final String tempDirPrefix;
    private final JsonSerializer<IN> jsonSerializer;

    private final List<PinotWriterSegment<IN>> activeSegments;
    private final FileSystemAdapter fsAdapter;

    private final int subtaskId;

    /**
     * @param subtaskId          Subtask id provided by Flink
     * @param maxRowsPerSegment  Maximum number of rows to be stored within a Pinot segment
     * @param eventTimeExtractor Defines the way event times are extracted from received objects
     * @param tempDirPrefix      Prefix for temp directories used
     * @param jsonSerializer     Serializer used to convert elements to JSON
     * @param fsAdapter          Filesystem adapter used to save files for sharing files across nodes
     */
    public PinotSinkWriter(int subtaskId, int maxRowsPerSegment, EventTimeExtractor<IN> eventTimeExtractor, String tempDirPrefix, JsonSerializer<IN> jsonSerializer, FileSystemAdapter fsAdapter) {
        this.subtaskId = subtaskId;
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.activeSegments = new ArrayList<>();
    }

    /**
     * Takes elements from an upstream tasks and writes them into {@link PinotWriterSegment}
     *
     * @param element Object from upstream task
     * @param context SinkWriter context
     * @throws IOException
     */
    @Override
    public void write(IN element, Context context) throws IOException {
        final PinotWriterSegment<IN> inProgressSegment = this.getOrCreateInProgressSegment();
        inProgressSegment.write(element, this.eventTimeExtractor.getEventTime(element, context));
    }

    /**
     * Creates {@link PinotSinkCommittable}s from elements received via {@link #write}
     *
     * @param flush Flush all currently known elements into the {@link PinotSinkCommittable}s
     * @return List of {@link PinotSinkCommittable} to process in {@link org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter}
     * @throws IOException
     */
    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<PinotWriterSegment<IN>> segmentsToCommit = this.activeSegments.stream()
                .filter(s -> flush || !s.acceptsElements())
                .collect(Collectors.toList());
        LOG.info("Identified {} segments to commit [subtaskId={}]", segmentsToCommit.size(), this.subtaskId);

        LOG.info("Creating committables... [subtaskId={}]", subtaskId);
        List<PinotSinkCommittable> committables = new ArrayList<>();
        for (final PinotWriterSegment<IN> segment : segmentsToCommit) {
            committables.add(segment.prepareCommit());
        }
        LOG.info("Created {} committables [subtaskId={}]", committables.size(), subtaskId);

        this.activeSegments.removeAll(segmentsToCommit);
        return committables;
    }

    /**
     * Gets the {@link PinotWriterSegment} still accepting elements or creates a new one.
     *
     * @return {@link PinotWriterSegment} accepting at least one more element
     */
    private PinotWriterSegment<IN> getOrCreateInProgressSegment() {
        final PinotWriterSegment<IN> latestSegment = Iterables.getLast(this.activeSegments, null);
        if (latestSegment == null || !latestSegment.acceptsElements()) {
            final PinotWriterSegment<IN> inProgressSegment = new PinotWriterSegment<>(this.maxRowsPerSegment, this.tempDirPrefix, this.jsonSerializer, this.fsAdapter);
            this.activeSegments.add(inProgressSegment);
            return inProgressSegment;
        }
        return latestSegment;
    }

    /**
     * Snapshots the current state to be stored within a checkpoint. As we do not need to save any
     * information in snapshots, this method always returns an empty ArrayList.
     *
     * @return always an empty ArrayList
     */
    @Override
    public List<Void> snapshotState() {
        return new ArrayList<>();
    }

    /**
     * Empty method, as we do not open any connections.
     */
    @Override
    public void close() {
    }
}
