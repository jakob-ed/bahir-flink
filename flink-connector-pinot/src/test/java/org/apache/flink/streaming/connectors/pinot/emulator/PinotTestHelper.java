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
import java.util.Arrays;
import java.util.List;

/**
 * Helper class ot interact with the Pinot components in the e2e tests
 */
public class PinotTestHelper extends PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotTestHelper.class);
    private final String brokerPort;

    public PinotTestHelper(String host, String controllerPort, String brokerPort) {
        super(host, controllerPort);
        this.brokerPort = brokerPort;
        System.out.println("PinotHelper with " + controllerHost + "  and  " + controllerPort);
    }

    private void addSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.post("/schemas", JsonUtils.objectToString(tableSchema));
        LOG.info("Schema add request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void deleteSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.delete(String.format("/schemas/%s", tableSchema.getSchemaName()));
        LOG.info("Schema delete request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void addTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.post("/tables", JsonUtils.objectToString(tableConfig));
        LOG.info("Table creation request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void removeTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.delete(String.format("/tables/%s", tableConfig.getTableName()));
        LOG.info("Table deletion request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    public void createTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.addSchema(tableSchema);
        this.addTable(tableConfig);
    }

    public void deleteTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.removeTable(tableConfig);
        this.deleteSchema(tableSchema);
    }

    public List<String> getBrokers(String tableName) throws IOException {
        ApiResponse res = this.get(String.format("/brokers/tables/%s", tableName));
        LOG.info("Get broker request for table {} returned {}", tableName, res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }

        List<String> brokers;
        try {
            brokers = Arrays.asList(JsonUtils.stringToObject(res.responseBody, String[].class));
        } catch (Exception e) {
            throw new IllegalStateException("Caught exception while reading brokers from Pinot Controller's response: " + res.responseBody, e);
        }
        LOG.info("Retrieved brokers: {}", brokers);

        return brokers;
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
