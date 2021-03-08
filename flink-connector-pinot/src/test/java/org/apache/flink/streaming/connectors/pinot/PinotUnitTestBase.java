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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.docker.client.exceptions.DockerException;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.pinot.emulator.PinotHelper;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.util.TestLogger;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class PinotUnitTestBase extends TestLogger implements Serializable {
    @BeforeClass
    public static void launchPinotEmulator() throws Exception {
        // Separated out into separate class so the entire test class to be serializable
        // PinotEmulatorManager.launchDocker();
    }

    @AfterAll
    public static void terminatePinotEmulator() throws DockerException, InterruptedException {
        // PinotEmulatorManager.terminateDocker();
    }

    // ====================================================================================
    // Pinot helpers

    public static PinotHelper getPinotHelper() {
        return new PinotHelper(getPinotControllerHost(), getPinotControllerPort());
    }

    public static String getPinotControllerHost() {
        return "127.0.0.1";
        // getDockerIpAddress() + ":" + getDockerPinotControllerPort();
    }

    public static String getPinotControllerPort() {
        return "9000";
        // getDockerIpAddress() + ":" + getDockerPinotControllerPort();
    }

    static class SingleColumnTableRow {

        private String _col1;
        private Long _timestamp;

        SingleColumnTableRow(@JsonProperty(value = "col1", required = true) String col1,
                             @JsonProperty(value = "timestamp", required = true) Long timestamp) {
            this._col1 = col1;
            this._timestamp = timestamp;
        }

        @JsonProperty("col1")
        public String getCol1() {
            return this._col1;
        }

        public void setCol1(String _col1) {
            this._col1 = _col1;
        }

        @JsonProperty("timestamp")
        public Long getTimestamp() { return this._timestamp; }

        public void setTimestamp(Long timestamp) { this._timestamp = timestamp; }
    }

    static class SingleColumnTableRowEventTimeExtractor extends EventTimeExtractor<SingleColumnTableRow> {

        @Override
        public long getEventTime(SingleColumnTableRow element, SinkWriter.Context context) {
            return element.getTimestamp();
        }

        @Override
        public String getTimeColumn() {
            return "timestamp";
        }

        @Override
        public TimeUnit getSegmentTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }
    }

    static class PinotTableConfig {

        static final String TABLE_NAME = "FLTable";
        static final String SCHEMA_NAME = "FLTableSchema";

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
            schema.addField(new DimensionFieldSpec("col1", FieldSpec.DataType.STRING, true));
            schema.addField(new DimensionFieldSpec("timestamp", FieldSpec.DataType.STRING, true));
            return schema;
        }
    }
}
