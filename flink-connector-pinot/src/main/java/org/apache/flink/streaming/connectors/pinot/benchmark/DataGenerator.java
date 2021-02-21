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


import org.apache.flink.streaming.connectors.pinot.benchmark.generator.TupleGenerator;
import org.apache.flink.streaming.connectors.pinot.benchmark.generator.TupleSender;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "DataGenerator", mixinStandardHelpOptions = true,
        description = "Start the data generator.")
public class DataGenerator implements Callable<Integer> {

    @CommandLine.Option(names = "--numTuples", required = true, description = "The overall number of tuples to send.")
    private int numTuples;

    @CommandLine.Option(names = "--sleepTime", required = true, description = "Time to sleep between tuple send.")
    private long sleepTime;

    @CommandLine.Option(names = "--bufferSize", required = true, description = "Size of tuples buffer.")
    private int bufferSize;

    @CommandLine.Option(names = "--port", required = true, description = "The port.")
    private int port;


    @Override
    public Integer call() {
        BlockingQueue<String> buffer = new ArrayBlockingQueue<>(bufferSize);

        try {
            ServerSocket serverSocket = new ServerSocket(this.port);
            serverSocket.setSoTimeout(900000);
            System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();
            System.out.println("Just connected to " + server.getRemoteSocketAddress());
            PrintWriter out = new PrintWriter(server.getOutputStream(), true);

            Thread generator = new TupleGenerator(this.numTuples, this.sleepTime, buffer);
            generator.start();

            Thread sender = new TupleSender(buffer, this.numTuples, out, serverSocket);
            sender.start();

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }
}
