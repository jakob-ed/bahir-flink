package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

/**
 * Immutable configuration for PinotSink
 *
 * @param <IN> type of input messages in configured sink
 */
public class PinotSinkConfig<IN> {

    private final PinotControllerConnectionFactory connectionFactory;
    private final String tableName;
    private final SerializationSchema<IN> serializationSchema;

    public PinotSinkConfig(PinotControllerConnectionFactory connectionFactory, String tableName,
                           SerializationSchema<IN> serializationSchema) {
        this.connectionFactory = Preconditions.checkNotNull(connectionFactory, "connectionFactory not set");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName not set");
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
    }

    public PinotControllerConnectionFactory getConnectionFactory() {
        return connectionFactory;
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
    public static class PinotSinkConfigBuilder<IN> {
        private PinotControllerConnectionFactory connectionFactory;
        private String tableName;
        private SerializationSchema<IN> serializationSchema;

        public PinotSinkConfigBuilder<IN> setConnectionFactory(PinotControllerConnectionFactory connectionFactory) {
            this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
            return this;
        }

        public PinotSinkConfigBuilder<IN> setTableName(String tableName) {
            this.tableName = Preconditions.checkNotNull(tableName);
            return this;
        }

        public PinotSinkConfigBuilder<IN> setSerializationSchema(SerializationSchema<IN> serializationSchema) {
            this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
            return this;
        }

        public PinotSinkConfig<IN> build() {
            return new PinotSinkConfig<IN>(connectionFactory, tableName, serializationSchema);
        }
    }
}
