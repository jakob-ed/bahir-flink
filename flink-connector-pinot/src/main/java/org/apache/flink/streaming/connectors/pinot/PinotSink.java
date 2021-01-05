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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class PinotSink<Row> implements Sink<Row, PinotSegmentConfig, PinotSegmentOffset, Object> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PinotSink.class);


    private final PinotControllerConnectionFactory connectionFactory;
    // Name of the destination table
    private final String tableName;
    // Serialization scheme that is used to convert input message to bytes
    private final SerializationSchema<Row> serializationSchema;

    /**
     * Create PinotSink.
     *
     * @param config PinotSink configuration
     */
    public PinotSink(PinotSinkConfig<Row> config) {
        this.connectionFactory = config.getConnectionFactory();
        this.tableName = config.getTableName();
        this.serializationSchema = config.getSerializationSchema();
    }

    @Override
    public PinotSinkWriter<Row> createWriter(InitContext initContext, List list) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Optional<Committer<PinotSegmentConfig>> createCommitter() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Optional<GlobalCommitter<PinotSegmentConfig, Object>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer> getCommittableSerializer() {
        throw new NotImplementedException();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Object>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer> getWriterStateSerializer() {
        throw new NotImplementedException();
    }
}
