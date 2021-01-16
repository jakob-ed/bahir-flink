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
import org.apache.flink.streaming.connectors.pinot.serializer.PinotSinkCommittableSerializer;
import org.apache.flink.streaming.connectors.pinot.serializer.PinotWriterStateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSink<IN> implements Sink<IN, PinotSinkCommittable, PinotWriterState, Void> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PinotSink.class);


    private final String pinotControllerHost;
    private final String pinotControllerPort;
    // Name of the destination table
    private final String tableName;

    /**
     * Create PinotSink.
     *
     * @param pinotControllerHost
     * @param pinotControllerPort
     * @param tableName
     */
    public PinotSink(String pinotControllerHost, String pinotControllerPort, String tableName) throws IOException {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = tableName;
    }

    @Override
    public PinotSinkWriter<IN> createWriter(InitContext context, List<PinotWriterState> states) throws IOException {
        PinotSinkWriter<IN> writer = new PinotSinkWriter<>(context.getSubtaskId(), this.pinotControllerHost, this.pinotControllerPort, this.tableName);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<PinotSinkCommittable>> createCommitter() {
        PinotSinkCommitter committer = new PinotSinkCommitter(this.pinotControllerHost, this.pinotControllerPort);
        return Optional.of(committer);
    }

    @Override
    public Optional<GlobalCommitter<PinotSinkCommittable, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PinotSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new PinotSinkCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PinotWriterState>> getWriterStateSerializer() {
        return Optional.of(new PinotWriterStateSerializer());
    }
}
