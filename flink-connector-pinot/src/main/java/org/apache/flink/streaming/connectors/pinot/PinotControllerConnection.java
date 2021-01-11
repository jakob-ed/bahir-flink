package org.apache.flink.streaming.connectors.pinot;

import org.apache.http.entity.mime.MultipartEntityBuilder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotControllerConnection {

    private final HttpURLConnection connection;

    public PinotControllerConnection(URL controllerUrl) throws IOException {
        checkNotNull(controllerUrl);
        this.connection = (HttpURLConnection) controllerUrl.openConnection();
    }

    public Boolean postSegmentFile(File segmentFile) throws IOException {
        MultipartEntityBuilder mb = MultipartEntityBuilder.create();
        mb.addBinaryBody("file", segmentFile);
        org.apache.http.HttpEntity e = mb.build();

        this.connection.setDoOutput(true);
        this.connection.addRequestProperty(e.getContentType().getName(), e.getContentType().getValue());
        this.connection.addRequestProperty("Content-Length", String.valueOf(e.getContentLength()));
        OutputStream fout = this.connection.getOutputStream();
        e.writeTo(fout);
        fout.close();
        this.connection.getInputStream().close();

        return true;
    }

    public void close() {
        this.connection.disconnect();
    }

    static class Factory {

        public static PinotControllerConnection createConnection(String controllerHostPort) throws IOException {
            return new PinotControllerConnection(new URL(controllerHostPort));
        }
    }
}
