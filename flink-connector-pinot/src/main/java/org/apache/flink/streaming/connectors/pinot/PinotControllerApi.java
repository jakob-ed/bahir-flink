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

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helpers to interact with the Pinot controller via its public API.
 */
public class PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotControllerApi.class);
    protected final String controllerHostPort;

    /**
     * @param controllerHost Pinot controller's host
     * @param controllerPort Pinot controller's port
     */
    public PinotControllerApi(String controllerHost, String controllerPort) {
        checkNotNull(controllerHost);
        checkNotNull(controllerPort);
        this.controllerHostPort = String.format("http://%s:%s", controllerHost, controllerPort);
    }

    /**
     * Issues a request to the Pinot controller API.
     *
     * @param request Request to issue
     * @return Api response
     * @throws IOException
     */
    private ApiResponse execute(HttpRequestBase request) throws IOException {
        ApiResponse result;

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(request)) {


            String body = EntityUtils.toString(response.getEntity());
            result = new ApiResponse(response.getStatusLine(), body);
        }

        return result;
    }

    /**
     * Issues a POST request to the Pinot controller API.
     *
     * @param path Path to POST to
     * @param body Request's body
     * @return API response
     * @throws IOException
     */
    protected ApiResponse post(String path, String body) throws IOException {
        HttpPost httppost = new HttpPost(this.controllerHostPort + path);
        httppost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        LOG.info("Posting string entity {} to {}", body, path);
        return this.execute(httppost);
    }

    /**
     * Issues a POST request to the Pinot controller API.
     *
     * @param path   Path to POST to
     * @param entity Http entity to POST
     * @return API response
     * @throws IOException
     */
    protected ApiResponse post(String path, HttpEntity entity) throws IOException {
        HttpPost httppost = new HttpPost(this.controllerHostPort + path);
        httppost.setEntity(entity);
        LOG.info("Posting {} entity to {}", entity.getContentType(), path);
        return this.execute(httppost);
    }

    /**
     * Issues a GET request to the Pinot controller API.
     *
     * @param path Path to GET from
     * @return API response
     * @throws IOException
     */
    protected ApiResponse get(String path) throws IOException {
        HttpGet httpget = new HttpGet(this.controllerHostPort + path);
        LOG.info("Sending GET request to {}", path);
        return this.execute(httpget);
    }

    /**
     * Issues a DELETE request to the Pinot controller API.
     *
     * @param path Path to issue DELETE request to
     * @return API response
     * @throws IOException
     */
    protected ApiResponse delete(String path) throws IOException {
        HttpDelete httpdelete = new HttpDelete(this.controllerHostPort + path);
        LOG.info("Sending DELETE request to {}", path);
        return this.execute(httpdelete);
    }

    /**
     * Fetches all segment names for the given table name from the Pinot controller.
     *
     * @param tableName Target table's name
     * @return List of segment names
     * @throws IOException
     */
    public List<String> getSegmentNames(String tableName) throws IOException {
        List<String> segmentNames = new ArrayList<>();
        ApiResponse res = this.get(String.format("/segments/%s", tableName));

        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }

        JsonNode parsedResponse = JsonUtils.stringToJsonNode(res.responseBody);
        for (int i = 0; i < parsedResponse.size(); i++) {
            JsonNode item = parsedResponse.get(i);
            if (item.has("OFFLINE")) {
                List<String> extracted = this.toListOfString(item.get("OFFLINE"));
                segmentNames.addAll(extracted);
            }
            if (item.has("REALTIME")) {
                List<String> extracted = this.toListOfString(item.get("REALTIME"));
                segmentNames.addAll(extracted);
            }
        }

        return segmentNames;
    }

    /**
     * Fetches a segment's metadata via the Pinot controller API.
     *
     * @param tableName   Target table's name
     * @param segmentName Segment name to fetch metadata for
     * @return Metadata as JsonNode
     * @throws IOException
     */
    public JsonNode getSegmentMetadata(String tableName, String segmentName) throws IOException {
        ApiResponse res = this.get(String.format("/segments/%s/%s/metadata", tableName, segmentName));

        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }

        return JsonUtils.stringToJsonNode(res.responseBody);
    }

    /**
     * Checks whether the provided segment name is registered with the given table.
     *
     * @param tableName   Target table's name
     * @param segmentName Segment name to check
     * @return True if segment with the provided name exists
     * @throws IOException
     */
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

    /**
     * Deletes a segment from a table.
     *
     * @param tableName   Target table's name
     * @param segmentName Identifies the segment to delete
     * @throws IOException
     */
    public void deleteSegment(String tableName, String segmentName) throws IOException {
        ApiResponse res = this.delete(String.format("/tables/%s/%s", tableName, segmentName));

        if (res.statusLine.getStatusCode() != 200) {
            LOG.error("Could not delete segment {} from table {}. Pinot controller returned: {}", tableName, segmentName, res.responseBody);
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    /**
     * Fetches a Pinot table's schema via the Pinot controller API.
     *
     * @param tableName Target table's name
     * @return Pinot table schema
     * @throws IOException
     */
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

    /**
     * Fetches a Pinot table's configuration via the Pinot controller API.
     *
     * @param tableName Target table's name
     * @return Pinot table configuration
     * @throws IOException
     */
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


    /**
     * Helper method to convert a json node to a list of strings.
     *
     * @param jsonNode JsonNode to convert
     * @return Content as list of strings
     */
    private List<String> toListOfString(JsonNode jsonNode) {
        List<String> result = new ArrayList<>();
        for (int j = 0; j < jsonNode.size(); j++) {
            String segmentName = jsonNode.get(j).asText();
            result.add(segmentName);
        }
        return result;
    }

    /**
     * Helper class for wrapping Pinot controller API responses.
     */
    protected class ApiResponse {
        public final StatusLine statusLine;
        public final String responseBody;

        public ApiResponse(StatusLine statusLine, String responseBody) {
            this.statusLine = statusLine;
            this.responseBody = responseBody;
        }
    }
}
