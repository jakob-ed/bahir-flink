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
import org.apache.flink.streaming.connectors.pinot.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, Void> {

    private static final Logger LOG = LoggerFactory.getLogger("PinotSinkWriter");

    private final Integer rowsPerSegment;
    private EventTimeExtractor<IN> eventTimeExtractor;

    private final List<PinotWriterSegment<IN>> activeSegments;
    private final FileSystemAdapter fsAdapter;

    public PinotSinkWriter(int rowsPerSegment, EventTimeExtractor<IN> eventTimeExtractor, FileSystemAdapter fsAdapter) {
        this.rowsPerSegment = checkNotNull(rowsPerSegment);
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.activeSegments = new ArrayList<>();
    }

    @Override
    public void write(IN element, Context context) {
        final PinotWriterSegment<IN> inProgressSegment = this.getOrCreateInProgressSegment();
        inProgressSegment.write(element, this.eventTimeExtractor.getEventTime(element, context));
    }

    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<PinotWriterSegment<IN>> segmentsToCommit = this.activeSegments.stream()
                .filter(s -> flush || !s.acceptsElements())
                .collect(Collectors.toList());
        LOG.info("Identified {} segments to commit", segmentsToCommit.size());

        LOG.info("Creating committables...");
        List<PinotSinkCommittable> committables = new ArrayList<>();
        for (final PinotWriterSegment<IN> segment : segmentsToCommit) {
            committables.add(segment.prepareCommit());
        }
        LOG.info("Created {} committables", committables.size());

        this.activeSegments.removeAll(segmentsToCommit);
        return committables;
    }

    private PinotWriterSegment<IN> getOrCreateInProgressSegment() {
        final PinotWriterSegment<IN> latestSegment = Iterables.getLast(this.activeSegments, null);
        if (latestSegment == null || !latestSegment.acceptsElements()) {
            final PinotWriterSegment<IN> inProgressSegment = new PinotWriterSegment<>(this.rowsPerSegment, this.fsAdapter);
            this.activeSegments.add(inProgressSegment);
            return inProgressSegment;
        }
        return latestSegment;
    }


    @Override
    public List<Void> snapshotState() {
        // The PinotSinkWriter isn't stateful and thus does not require any state management
        return new ArrayList<>();
    }

    @Override
    public void close() {
    }
}
