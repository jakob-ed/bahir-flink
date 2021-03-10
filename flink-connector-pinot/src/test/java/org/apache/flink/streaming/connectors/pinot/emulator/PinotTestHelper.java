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

package org.apache.flink.streaming.connectors.pinot.emulator;

import org.apache.flink.streaming.connectors.pinot.PinotControllerApi;
import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.pinot.client.*;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Helper class ot interact with the Pinot controller and broker in the e2e tests
 */
public class PinotTestHelper extends PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotTestHelper.class);
    private final String brokerPort;

    /**
     * @param host           Host the Pinot controller and broker are accessible at
     * @param controllerPort The Pinot controller's external port at {@code host}
     * @param brokerPort     A Pinot broker's external port at {@code host}
     */
    public PinotTestHelper(String host, String controllerPort, String brokerPort) {
        super(host, controllerPort);
        this.brokerPort = brokerPort;
    }

    /**
     * Adds a Pinot table schema.
     *
     * @param tableSchema Pinot table schema to add
     * @throws IOException
     */
    private void addSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.post("/schemas", JsonUtils.objectToString(tableSchema));
        LOG.info("Schema add request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Deletes a Pinot table schema.
     *
     * @param tableSchema Pinot table schema to delete
     * @throws IOException
     */
    private void deleteSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.delete(String.format("/schemas/%s", tableSchema.getSchemaName()));
        LOG.info("Schema delete request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Creates a Pinot table.
     *
     * @param tableConfig Pinot table configuration of table to create
     * @throws IOException
     */
    private void addTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.post("/tables", JsonUtils.objectToString(tableConfig));
        LOG.info("Table creation request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Deletes a Pinot table with all its segments.
     *
     * @param tableConfig Pinot table configuration of table to delete
     * @throws IOException
     */
    private void removeTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.delete(String.format("/tables/%s", tableConfig.getTableName()));
        LOG.info("Table deletion request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Creates a Pinot table by first adding a schema and then creating the actual table using the
     * Pinot table configuration
     *
     * @param tableConfig Pinot table configuration
     * @param tableSchema Pinot table schema
     * @throws IOException
     */
    public void createTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.addSchema(tableSchema);
        this.addTable(tableConfig);
    }

    /**
     * Deletes a Pinot table by first deleting the table and its segments and then deleting the
     * table's schema.
     *
     * @param tableConfig Pinot table configuration
     * @param tableSchema Pinot table schema
     * @throws IOException
     */
    public void deleteTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.removeTable(tableConfig);
        this.deleteSchema(tableSchema);
    }

    /**
     * Fetch table entries via the Pinot broker.
     *
     * @param tableName          Target table's name
     * @param maxNumberOfEntries Max number of entries to fetch
     * @return ResultSet
     * @throws Exception
     */
    public ResultSet getTableEntries(String tableName, Integer maxNumberOfEntries) throws Exception {
        String brokerHostPort = String.format("%s:%s", this.controllerHost, this.brokerPort);
        Connection brokerConnection = ConnectionFactory.fromHostList(brokerHostPort);

        String query = String.format("SELECT * FROM %s LIMIT %d", tableName, maxNumberOfEntries);

        Request pinotClientRequest = new Request("sql", query);
        ResultSetGroup pinotResultSetGroup = brokerConnection.execute(pinotClientRequest);

        if (pinotResultSetGroup.getResultSetCount() != 1) {
            throw new Exception("Could not find any data in Pinot cluster.");
        }
        return pinotResultSetGroup.getResultSet(0);
    }
}
