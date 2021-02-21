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

import com.fasterxml.jackson.annotation.JsonProperty;

public class BenchmarkEvent {
    private String value;
    private String key;
    private String ts;

    public BenchmarkEvent(@JsonProperty(value = "value", required = true) String value,
                          @JsonProperty(value = "key", required = true) String key,
                          @JsonProperty(value = "ts", required = true) String ts) {
        this.value = value;
        this.key = key;
        this.ts = ts;
    }

    @JsonProperty("value")
    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @JsonProperty("ts")
    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
}
