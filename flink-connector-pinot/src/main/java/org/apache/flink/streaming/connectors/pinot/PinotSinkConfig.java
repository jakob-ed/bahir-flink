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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

/**
 * Immutable configuration for PinotSink
 *
 * @param <IN> type of input messages in configured sink
 */
public class PinotSinkConfig<IN> {

    private final String pinotControllerHostPort;
    private final String tableName;
    private final SerializationSchema<IN> serializationSchema;

    public PinotSinkConfig(String pinotControllerHostPort, String tableName,
                           SerializationSchema<IN> serializationSchema) {
        this.pinotControllerHostPort = Preconditions.checkNotNull(pinotControllerHostPort, "pinotControllerHostPort not set");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName not set");
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
    }

    public String getPinotControllerHostPort() {
        return pinotControllerHostPort;
    }

    public String getTableName() {
        return tableName;
    }

    public SerializationSchema<IN> getSerializationSchema() {
        return serializationSchema;
    }

    /**
     * Builder for {@link PinotSinkConfig}
     *
     * @param <IN> type of input messages in configured sink
     */
    public static class Builder<IN> {
        private String pinotControllerHostPort;
        private String tableName;
        private SerializationSchema<IN> serializationSchema;

        public PinotSinkConfig.Builder<IN> setPinotControllerHostPort(String pinotControllerHostPort) {
            this.pinotControllerHostPort = Preconditions.checkNotNull(pinotControllerHostPort);
            return this;
        }

        public PinotSinkConfig.Builder<IN> setTableName(String tableName) {
            this.tableName = Preconditions.checkNotNull(tableName);
            return this;
        }

        public PinotSinkConfig.Builder<IN> setSerializationSchema(SerializationSchema<IN> serializationSchema) {
            this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
            return this;
        }

        public PinotSinkConfig<IN> build() {
            return new PinotSinkConfig<IN>(pinotControllerHostPort, tableName, serializationSchema);
        }
    }
}
