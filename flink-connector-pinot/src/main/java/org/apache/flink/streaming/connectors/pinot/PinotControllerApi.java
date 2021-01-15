package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
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
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotControllerApi.class);
    protected final String controllerHostPost;

    public PinotControllerApi(String controllerHostPost) {
        this.controllerHostPost = controllerHostPost;
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
        HttpPost httppost = new HttpPost(this.controllerHostPost + path);
        httppost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        LOG.info("Posting string entity {} to {}", body, path);
        return this.execute(httppost);
    }

    protected ApiResponse get(String path) throws IOException {
        HttpGet httpget = new HttpGet(this.controllerHostPost + path);
        LOG.info("Sending GET request to {}", path);
        return this.execute(httpget);
    }

    protected ApiResponse delete(String path) throws IOException {
        HttpDelete httpdelete = new HttpDelete(this.controllerHostPost + path);
        LOG.info("Sending DELETE request to {}", path);
        return this.execute(httpdelete);
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

    public List<String> getTableEntries(String tableName, int maxNumberOfEntries) {
        return null;
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
