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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.pinot.external.EventTimeExtractor;
import org.apache.flink.streaming.connectors.pinot.external.JsonSerializer;
import org.apache.flink.util.TestLogger;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Base class for PinotSink e2e tests
 */
@Testcontainers
public class PinotTestBase extends TestLogger implements Serializable {

    public static final String DOCKER_IMAGE_NAME = "apachepinot/pinot:0.6.0";
    public static final Integer PINOT_INTERNAL_BROKER_PORT = 8000;
    public static final Integer PINOT_INTERNAL_CONTROLLER_PORT = 9000;

    protected static final TableConfig TABLE_CONFIG = PinotTableConfig.getTableConfig();
    protected static final String TABLE_NAME = TABLE_CONFIG.getTableName();
    protected static final Schema TABLE_SCHEMA = PinotTableConfig.getTableSchema();
    protected static PinotTestHelper pinotHelper;

    /**
     * Creates the Pinot testcontainer. We delay the start of tests until Pinot has started all
     * internal components. This is identified through a log statement.
     */
    @Container
    public GenericContainer<?> pinot = new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE_NAME))
            .withCommand("QuickStart", "-type", "batch")
            .withExposedPorts(PINOT_INTERNAL_BROKER_PORT, PINOT_INTERNAL_CONTROLLER_PORT)
            .waitingFor(Wait
                    .forLogMessage(".*You can always go to http://localhost:9000 to play around in the query console.*\\n", 1)
                    // Allow Pinot to take up to 90s for starting up
                    .withStartupTimeout(Duration.ofSeconds(90))
            );

    /**
     * Creates a new instance of the {@link PinotTestHelper} using the testcontainer port mappings
     * and creates the test table.
     *
     * @throws IOException
     */
    @BeforeEach
    public void setUp() throws IOException {
        pinotHelper = new PinotTestHelper(getPinotHost(), getPinotControllerPort(), getPinotBrokerPort());
        pinotHelper.createTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Delete the test table after each test.
     *
     * @throws Exception
     */
    @AfterEach
    public void tearDown() throws Exception {
        pinotHelper.deleteTable(TABLE_CONFIG, TABLE_SCHEMA);
    }

    /**
     * Returns the host the Pinot container is available at
     *
     * @return Pinot container host
     */
    public String getPinotHost() {
        return this.pinot.getHost();
    }


    /**
     * Returns the Pinot controller port from the container ports.
     *
     * @return Pinot controller port
     */
    public String getPinotControllerPort() {
        return this.pinot.getMappedPort(PINOT_INTERNAL_CONTROLLER_PORT).toString();
    }

    /**
     * Returns the Pinot broker port from the container ports.
     *
     * @return Pinot broker port
     */
    public String getPinotBrokerPort() {
        return this.pinot.getMappedPort(PINOT_INTERNAL_BROKER_PORT).toString();
    }

    /**
     * Class defining the elements passed to the {@link PinotSink} during the tests.
     */
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
        public Long getTimestamp() {
            return this._timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this._timestamp = timestamp;
        }
    }


    /**
     * EventTimeExtractor for {@link SingleColumnTableRow} used in e2e tests.
     * Extracts the timestamp column from {@link SingleColumnTableRow}.
     */
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

    /**
     * Serializes {@link SingleColumnTableRow} to JSON.
     */
    static class SingleColumnTableRowSerializer extends JsonSerializer<SingleColumnTableRow> {

        @Override
        public String toJson(SingleColumnTableRow element) {
            return JsonUtils.objectToJsonNode(element).toString();
        }
    }

    /**
     * Pinot table configuration helpers.
     */
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
