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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TupleGenerator extends Thread {
    private final int tupleCount;
    private final long sleepTime;
    private static Double partition;
    private final BlockingQueue<String> buffer;
    private final Event event;
    private final HashMap<Long, Integer> bufferSizeAtTime = new HashMap<>();

    private final HashMap<Long, Integer> dataGenRate = new HashMap<>();

    public TupleGenerator(int tupleCount, long sleepTime, BlockingQueue<String> buffer) throws IOException {
        this.buffer = buffer;
        this.tupleCount = tupleCount;
        this.sleepTime = sleepTime;

        partition = 1.0;
        event = new Event(partition);
    }

    public void run() {
        try {
            sendTuples(tupleCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount) throws Exception {
        long currTime = System.currentTimeMillis();
        int tempVal = 0;
        if (sleepTime != 0) {
            for (int i = 0; i < tupleCount; ) {
                if (i % 1000 == 0) {
                    Thread.sleep(sleepTime);
                }
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) {
                    buffer.put(event.generateJson());
                    if (i % 1000 == 0) {
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ) {
                for (int b = 0; b < 1 && i < tupleCount; b++, i++) {
                    buffer.put(event.generateJson());
                    if (i % 1000 == 0) {
                        long interval = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
                        int bufferSize = buffer.size();
                        bufferSizeAtTime.put(interval, bufferSize);
                        dataGenRate.put(interval, i - tempVal);
                        tempVal = i;
                    }
                }
            }
        }
        long runtime = (currTime - System.currentTimeMillis()) / 1000;
        System.out.println("Benchmark producer data rate is " + tupleCount / runtime + " ps");
    }
}
