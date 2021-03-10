package org.apache.flink.streaming.connectors.pinot.external;

import java.io.Serializable;

public abstract class JsonSerializer<IN> implements Serializable {

    public abstract String toJson(IN element);
}
