package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.Serializable;

public abstract class EventTimeExtractor<T> implements Serializable {

    public abstract Long getEventTime(T element, SinkWriter.Context context);
}
