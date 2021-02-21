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

package org.apache.flink.streaming.connectors.pinot.benchmark.generator;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TupleSender extends Thread {
    private final BlockingQueue<String> buffer;
    private final Logger logger = Logger.getLogger("MyLog");
    private final PrintWriter out;
    private final ServerSocket serverSocket;
    private final int tupleCount;
    private final HashMap<Long, Integer> thoughputCount = new HashMap<>();

    public TupleSender(BlockingQueue<String> buffer, int tupleCount, PrintWriter out, ServerSocket serverSocket) {
        this.buffer = buffer;
        this.out = out;
        this.serverSocket = serverSocket;
        this.tupleCount = tupleCount;
    }

    public void run() {
        try {
            long timeStart = System.currentTimeMillis();

            int tempVal = 0;
            for (int i = 0; i < tupleCount; i++) {
                String tuple = buffer.take();
                out.println(tuple);
                if (i % 1000 == 0) {
                    thoughputCount.put(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), i - tempVal);
                    tempVal = i;
                    logger.info(i + " tuples sent from buffer");
                }
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = this.tupleCount / runtime;

            logger.info("---BENCHMARK ENDED--- on " + runtime + " seconds with " + throughput + " throughput "
                    + " node : " + InetAddress.getLocalHost().getHostName());
            logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeHashMapToCsv(HashMap<Long, Integer> hm, String path) {
        try {
            File file = new File(path.split("\\.")[0] + "-" + InetAddress.getLocalHost().getHostName() + ".csv");

            if (file.exists()) {
                file.delete(); //you might want to check if delete was successfull
            }
            file.createNewFile();
            FileOutputStream fileOutput = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutput));
            Iterator it = hm.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry) it.next();
                bw.write(pair.getKey() + "," + pair.getValue() + "\n");
            }
            bw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
