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
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.segment.name.SimpleSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkGlobalCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink sink that pushes segments to the specified Pinot cluster.
 * Requires checkpointing to be enabled.
 *
 * @param <IN> Type of incoming elements
 */
public class PinotSink<IN> implements Sink<IN, PinotSinkCommittable, Void, PinotSinkGlobalCommittable> {

    private static final long serialVersionUID = 1L;

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final int maxRowsPerSegment;
    private final String tempDirPrefix;
    private final JsonSerializer<IN> jsonSerializer;
    private final SegmentNameGenerator segmentNameGenerator;
    private final FileSystemAdapter fsAdapter;
    private final EventTimeExtractor<IN> eventTimeExtractor;

    /**
     * @param pinotControllerHost  Host of the Pinot controller
     * @param pinotControllerPort  Port of the Pinot controller
     * @param tableName            Target table's name
     * @param maxRowsPerSegment    Maximum number of rows to be stored within a Pinot segment
     * @param tempDirPrefix        Prefix for temp directories used
     * @param jsonSerializer       Serializer used to convert elements to JSON
     * @param eventTimeExtractor   Defines the way event times are extracted from received objects
     * @param segmentNameGenerator Pinot segment name generator
     * @param fsAdapter            Filesystem adapter used to save files for sharing files across nodes
     */
    public PinotSink(String pinotControllerHost, String pinotControllerPort, String tableName, int maxRowsPerSegment, String tempDirPrefix, JsonSerializer<IN> jsonSerializer, EventTimeExtractor<IN> eventTimeExtractor, SegmentNameGenerator segmentNameGenerator, FileSystemAdapter fsAdapter) {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);

        checkArgument(maxRowsPerSegment > 0);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
        this.jsonSerializer = checkNotNull(jsonSerializer);
        this.eventTimeExtractor = checkNotNull(eventTimeExtractor);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
        this.fsAdapter = checkNotNull(fsAdapter);
    }

    /**
     * Creates a Pinot sink writer.
     *
     * @param context InitContext
     * @param states  Empty list as the PinotSinkWriter does not accept states.
     */
    @Override
    public PinotSinkWriter<IN> createWriter(InitContext context, List<Void> states) {
        return new PinotSinkWriter<>(context.getSubtaskId(), this.maxRowsPerSegment, this.eventTimeExtractor, this.tempDirPrefix, this.jsonSerializer, this.fsAdapter);
    }

    /**
     * The PinotSink does not use a committer. Instead a global committer is used
     *
     * @return Empty Optional
     */
    @Override
    public Optional<Committer<PinotSinkCommittable>> createCommitter() {
        return Optional.empty();
    }

    /**
     * Creates the global committer.
     */
    @Override
    public Optional<GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable>> createGlobalCommitter() {
        String timeColumnName = eventTimeExtractor.getTimeColumn();
        TimeUnit segmentTimeUnit = eventTimeExtractor.getSegmentTimeUnit();
        PinotSinkGlobalCommitter committer = new PinotSinkGlobalCommitter(this.pinotControllerHost, this.pinotControllerPort, this.tableName, this.segmentNameGenerator, this.fsAdapter, timeColumnName, segmentTimeUnit);
        return Optional.of(committer);
    }

    /**
     * Creates the committables' serializer.
     */
    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new PinotSinkCommittableSerializer());
    }

    /**
     * Creates the global committables' serializer.
     */
    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkGlobalCommittable>> getGlobalCommittableSerializer() {
        return Optional.of(new PinotSinkGlobalCommittableSerializer());
    }

    /**
     * The PinotSink does not use writer states.
     *
     * @return Empty Optional
     */
    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    /**
     *
     * @param <IN> Type of incoming elements
     */
    public static class Builder<IN> {
        String pinotControllerHost;
        String pinotControllerPort;
        String tableName;
        int maxRowsPerSegment;
        String tempDirPrefix = "flink-connector-pinot";
        JsonSerializer<IN> jsonSerializer;
        EventTimeExtractor<IN> eventTimeExtractor;
        SegmentNameGenerator segmentNameGenerator;
        FileSystemAdapter fsAdapter;

        public Builder(String pinotControllerHost, String pinotControllerPort, String tableName) {
            this.pinotControllerHost = pinotControllerHost;
            this.pinotControllerPort = pinotControllerPort;
            this.tableName = tableName;
        }

        public Builder<IN> withJsonSerializer(JsonSerializer<IN> jsonSerializer) {
            this.jsonSerializer = jsonSerializer;
            return this;
        }

        public Builder<IN> withEventTimeExtractor(EventTimeExtractor<IN> eventTimeExtractor) {
            this.eventTimeExtractor = eventTimeExtractor;
            return this;
        }

        public Builder<IN> withSegmentNameGenerator(SegmentNameGenerator segmentNameGenerator) {
            this.segmentNameGenerator = segmentNameGenerator;
            return this;
        }

        public Builder<IN> withSimpleSegmentNameGenerator(String tableName, String segmentNamePostfix) {
            return this.withSegmentNameGenerator(new SimpleSegmentNameGenerator(tableName, segmentNamePostfix));
        }

        public Builder<IN> withFileSystemAdapter(FileSystemAdapter fsAdapter) {
            this.fsAdapter = fsAdapter;
            return this;
        }

        public Builder<IN> withLocalFileSystemAdapter() {
            return this.withFileSystemAdapter(new LocalFileSystemAdapter());
        }

        public Builder<IN> withMaxRowsPerSegment(int maxRowsPerSegment) {
            this.maxRowsPerSegment = maxRowsPerSegment;
            return this;
        }

        public Builder<IN> withTempDirectoryPrefix(String tempDirPrefix) {
            this.tempDirPrefix = tempDirPrefix;
            return this;
        }

        public PinotSink<IN> build() {
            return new PinotSink<>(
                    this.pinotControllerHost,
                    this.pinotControllerPort,
                    this.tableName,
                    this.maxRowsPerSegment,
                    this.tempDirPrefix,
                    this.jsonSerializer,
                    this.eventTimeExtractor,
                    this.segmentNameGenerator,
                    this.fsAdapter
            );
        }
    }
}
