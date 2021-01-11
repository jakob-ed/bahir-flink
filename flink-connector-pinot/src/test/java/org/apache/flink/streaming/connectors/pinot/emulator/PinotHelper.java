package org.apache.flink.streaming.connectors.pinot.emulator;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PinotHelper {

    private static final Logger LOG = LoggerFactory.getLogger(PinotHelper.class);
    private final String controllerHostPost;

    public PinotHelper(String controllerHostPost) {
        this.controllerHostPost = controllerHostPost;
    }

    private String post(String path, String body) throws IOException {
        String result = "";
        HttpPost httppost = new HttpPost(this.controllerHostPost + path);
        httppost.setEntity(new StringEntity(body));

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(httppost)) {

            result = EntityUtils.toString(response.getEntity());
        }

        return result;
    }

    private String delete(String path) throws IOException {
        String result = "";
        HttpDelete httpdelete = new HttpDelete(this.controllerHostPost + path);

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(httpdelete)) {

            result = EntityUtils.toString(response.getEntity());
        }

        return result;
    }

    public void createTable(String tableName) throws IOException {
        String res = this.post("/tables", tableName);
        System.out.println(res);
    }

    public void deleteTable(String tableName) throws IOException {
        this.delete(String.format("/tables/{}", tableName));
    }

    public List<String> getTableEntries(String tableName, int maxNumberOfEntries) {
        return null;
    }
}
