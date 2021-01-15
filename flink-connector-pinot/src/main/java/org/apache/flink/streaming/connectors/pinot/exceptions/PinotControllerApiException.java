package org.apache.flink.streaming.connectors.pinot.exceptions;

import java.io.IOException;

public class PinotControllerApiException extends IOException {

    public PinotControllerApiException(String reason) {
        super(reason);
    }
}
