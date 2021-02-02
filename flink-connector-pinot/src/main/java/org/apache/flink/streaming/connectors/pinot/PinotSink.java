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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommitter;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkGlobalCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotWriterStateSerializer;
import org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter;
import org.apache.flink.streaming.connectors.pinot.writer.PinotWriterState;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSink<IN> implements Sink<IN, PinotSinkCommittable, PinotWriterState, PinotSinkGlobalCommittable> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PinotSink.class);


    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final Integer rowsPerSegment;
    private final SegmentNameGenerator segmentNameGenerator;

    /**
     * Create PinotSink.
     *
     * @param pinotControllerHost
     * @param pinotControllerPort
     * @param tableName
     * @param rowsPerSegment
     */
    public PinotSink(String pinotControllerHost, String pinotControllerPort, String tableName, Integer rowsPerSegment, SegmentNameGenerator segmentNameGenerator) throws IOException {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);

        checkArgument(rowsPerSegment > 0L);
        this.rowsPerSegment = rowsPerSegment;
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
    }

    @Override
    public PinotSinkWriter<IN> createWriter(InitContext context, List<PinotWriterState> states) throws IOException {
        PinotSinkWriter<IN> writer = new PinotSinkWriter<>(this.rowsPerSegment);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<PinotSinkCommittable>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable>> createGlobalCommitter() {
        PinotSinkGlobalCommitter committer = new PinotSinkGlobalCommitter(this.pinotControllerHost, this.pinotControllerPort, this.tableName, this.segmentNameGenerator);
        return Optional.of(committer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new PinotSinkCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkGlobalCommittable>> getGlobalCommittableSerializer() {
        return Optional.of(new PinotSinkGlobalCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<PinotWriterState>> getWriterStateSerializer() {
        return Optional.of(new PinotWriterStateSerializer());
    }
}
