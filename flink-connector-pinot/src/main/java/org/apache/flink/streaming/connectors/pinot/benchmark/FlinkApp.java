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


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pinot.PinotSegmentNameGenerator;
import org.apache.flink.streaming.connectors.pinot.PinotSink;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.flink.streaming.connectors.pinot.filesystem.LocalFileSystemAdapter;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import picocli.CommandLine;

import java.util.Collections;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkApp", description = "Starts the benchmark app.")
public class FlinkApp implements Callable<Integer> {

    private String TABLE_NAME = "flink-pinot-connector-benchmark";
    private String PINOT_CONTROLLER_HOST = "pinot-cluster";
    private String PINOT_CONTROLLER_PORT = "9000";


    @CommandLine.Option(names = "--parallelism", required = true,
            description = "The num of partitions to execute the Flink application with.")
    private Integer parallelism = 1;

    @CommandLine.Option(names = "--segmentSize", required = true,
            description = "The number of tuples per segment.")
    private Integer segmentSize = 100;

    @CommandLine.Option(names = "--port", required = true, description = "The source port.")
    private Integer port;

    @Override
    public Integer call() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = this.setupSource(env);
        this.setupSink(dataStream);

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

    private void setupSink(DataStream<String> dataStream) {
        SegmentNameGenerator segmentNameGenerator = new PinotSegmentNameGenerator(TABLE_NAME, "flink-connector");
        FileSystemAdapter fsAdapter = new LocalFileSystemAdapter("flink-pinot-connector-benchmark");

        // Sink into Pinot
        dataStream.sinkTo(new PinotSink<>(PINOT_CONTROLLER_HOST, PINOT_CONTROLLER_PORT, TABLE_NAME, segmentSize, segmentNameGenerator, fsAdapter))
                .name("Pinot Sink");
    }
}
