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

package org.apache.flink.streaming.connectors.pinot.benchmark;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.PinotSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.PinotSink;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkApp", description = "Starts the benchmark app.")
public class FlinkApp implements Callable<Integer> {

    private String PINOT_CONTROLLER_HOST = "pinot-cluster";
    private String PINOT_CONTROLLER_PORT = "9000";


    @CommandLine.Option(names = "--parallelism", required = true,
            description = "The num of partitions to execute the Flink application with.")
    private Integer parallelism;

    @CommandLine.Option(names = "--segmentSize", required = true,
            description = "The number of tuples per segment.")
    private Integer segmentSize;

    @CommandLine.Option(names = "--port", required = true, description = "The source port.")
    private Integer port;

    @CommandLine.Option(names = "--delay", required = true, description = "Startup delay.")
    private Long delay;

    @Override
    public Integer call() throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(this.parallelism);
        env.enableCheckpointing(10000);

        DataStream<BenchmarkEvent> dataStream = this.setupSource(env)
                .map(message -> JsonUtils.stringToObject(message, BenchmarkEvent.class))
                .map(message -> {
                    System.out.println("Received tuple @Flink " + message.toString());
                    return message;
                });
        this.setupSink(dataStream);

        // Wait before trying to connect to the Pinot controller
        Thread.sleep(delay);

        this.setupPinot();

        env.execute();
        return 0;
    }

    private DataStream<String> setupSource(StreamExecutionEnvironment env) {
        DataStream<String> socketSource = null;
        for (String host : Collections.singletonList("data-generator")) {
            for (int port : Collections.singletonList(this.port)) {
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }

        return socketSource;
    }

    private void setupSink(DataStream<BenchmarkEvent> dataStream) {
        SegmentNameGenerator segmentNameGenerator = new PinotSegmentNameGenerator(PinotTableConfig.TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter("flink-pinot-connector-benchmark");

        BenchmarkEventTimeExtractor eventTimeExtractor = new BenchmarkEventTimeExtractor();
        dataStream.sinkTo(new PinotSink<>(PINOT_CONTROLLER_HOST, PINOT_CONTROLLER_PORT, PinotTableConfig.TABLE_NAME, segmentSize, eventTimeExtractor, segmentNameGenerator, fsAdapter))
                .name("Pinot Sink");
    }

    private void setupPinot() throws IOException {
        PinotHelper pinotHelper = new PinotHelper(PINOT_CONTROLLER_HOST, PINOT_CONTROLLER_PORT);
        pinotHelper.createTable(PinotTableConfig.getTableConfig(), PinotTableConfig.getTableSchema());
    }

    static class PinotTableConfig {

        static final String TABLE_NAME = "BenchmarkTable";
        static final String SCHEMA_NAME = "BenchmarkTableSchema";

        private static SegmentsValidationAndRetentionConfig getValidationConfig() {
            SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
            validationConfig.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
            validationConfig.setSegmentPushType("APPEND");
            validationConfig.setSchemaName(SCHEMA_NAME);
            validationConfig.setReplication("1");
            return validationConfig;
        }

        private static TenantConfig getTenantConfig() {
            TenantConfig tenantConfig = new TenantConfig("DefaultTenant", "DefaultTenant", null);
            return tenantConfig;
        }

        private static IndexingConfig getIndexingConfig() {
            IndexingConfig indexingConfig = new IndexingConfig();
            return indexingConfig;
        }

        private static TableCustomConfig getCustomConfig() {
            TableCustomConfig customConfig = new TableCustomConfig(null);
            ;
            return customConfig;
        }

        static TableConfig getTableConfig() {
            return new TableConfig(
                    TABLE_NAME,
                    TableType.OFFLINE.name(),
                    getValidationConfig(),
                    getTenantConfig(),
                    getIndexingConfig(),
                    getCustomConfig(),
                    null, null, null, null, null,
                    null, null, null, null
            );
        }

        static Schema getTableSchema() {
            Schema schema = new Schema();
            schema.setSchemaName(SCHEMA_NAME);
            schema.addField(new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true));
            schema.addField(new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true));
            schema.addField(new DimensionFieldSpec("ts", FieldSpec.DataType.STRING, true));
            return schema;
        }
    }
}
