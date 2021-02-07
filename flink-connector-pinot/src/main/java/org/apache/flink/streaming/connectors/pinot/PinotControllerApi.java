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

import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotControllerApi.class);
    protected final String controllerHostPort;

    public PinotControllerApi(String controllerHost, String controllerPort) {
        checkNotNull(controllerHost);
        checkNotNull(controllerPort);
        this.controllerHostPort = String.format("http://%s:%s", controllerHost, controllerPort);
    }

    private ApiResponse execute(HttpRequestBase request) throws IOException {
        ApiResponse result;

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(request)) {


            String body = EntityUtils.toString(response.getEntity());
            result = new ApiResponse(response.getStatusLine(), body);
        }

        return result;
    }

    protected ApiResponse post(String path, String body) throws IOException {
        HttpPost httppost = new HttpPost(this.controllerHostPort + path);
        httppost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        LOG.info("Posting string entity {} to {}", body, path);
        return this.execute(httppost);
    }

    protected ApiResponse post(String path, HttpEntity entity) throws IOException {
        HttpPost httppost = new HttpPost(this.controllerHostPort + path);
        httppost.setEntity(entity);
        LOG.info("Posting {} entity to {}", entity.getContentType(), path);
        return this.execute(httppost);
    }

    protected ApiResponse get(String path) throws IOException {
        HttpGet httpget = new HttpGet(this.controllerHostPort + path);
        LOG.info("Sending GET request to {}", path);
        return this.execute(httpget);
    }

    protected ApiResponse delete(String path) throws IOException {
        HttpDelete httpdelete = new HttpDelete(this.controllerHostPort + path);
        LOG.info("Sending DELETE request to {}", path);
        return this.execute(httpdelete);
    }

    public boolean tableHasSegment(String tableName, String segmentName) throws IOException {
        ApiResponse res = this.get(String.format("/tables/%s/%s/metadata", tableName, segmentName));

        if (res.statusLine.getStatusCode() == 200) {
            // A segment named `segmentName` exists within the table named `tableName`
            return true;
        }
        if (res.statusLine.getStatusCode() == 404) {
            // There is no such segment named `segmentName` within the table named `tableName`
            // (or the table named `tableName` does not exist)
            return false;
        }

        // Received an unexpected status code
        throw new PinotControllerApiException(res.responseBody);
    }

    public void deleteSegment(String tableName, String segmentName) throws IOException {
        ApiResponse res = this.delete(String.format("/tables/%s/%s", tableName, segmentName));

        if (res.statusLine.getStatusCode() != 200) {
            LOG.error("Could not delete segment {} from table {}. Pinot controller returned: {}", tableName, segmentName, res.responseBody);
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    public Schema getSchema(String tableName) throws IOException {
        Schema schema;
        ApiResponse res = this.get(String.format("/tables/%s/schema", tableName));
        LOG.info("Get schema request for table {} returned {}", tableName, res.responseBody);

        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }

        try {
            schema = JsonUtils.stringToObject(res.responseBody, Schema.class);
        } catch (Exception e) {
            throw new IllegalStateException("Caught exception while reading schema from Pinot Controller's response: " + res.responseBody, e);
        }
        LOG.info("Retrieved schema: {}", schema.toSingleLineJsonString());
        return schema;
    }

    public TableConfig getTableConfig(String tableName) throws IOException {
        TableConfig tableConfig;
        ApiResponse res = this.get(String.format("/tables/%s", tableName));
        LOG.info("Get table config request for table {} returned {}", tableName, res.responseBody);

        try {
            String tableConfigAsJson = JsonUtils.stringToJsonNode(res.responseBody).get("OFFLINE").toString();
            tableConfig = JsonUtils.stringToObject(tableConfigAsJson, TableConfig.class);
        } catch (Exception e) {
            throw new IllegalStateException("Caught exception while reading table config from Pinot Controller's response: " + res.responseBody, e);
        }
        LOG.info("Retrieved table config: {}", tableConfig.toJsonString());
        return tableConfig;
    }

    protected class ApiResponse {
        public final StatusLine statusLine;
        public final String responseBody;

        public ApiResponse(StatusLine statusLine, String responseBody) {
            this.statusLine = statusLine;
            this.responseBody = responseBody;
        }
    }
}
