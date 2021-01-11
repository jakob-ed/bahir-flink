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

        public PinotSinkConfig.Builder<IN> SetPinotControllerHostPort(String pinotControllerHostPort) {
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
