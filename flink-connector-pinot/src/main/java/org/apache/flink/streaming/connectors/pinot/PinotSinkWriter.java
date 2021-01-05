package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.SinkWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;

public class PinotSinkWriter<Row> implements SinkWriter<Row, PinotSegmentConfig, PinotSegmentOffset> {
    @Override
    public void write(Row row, Context context) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public List<PinotSegmentConfig> prepareCommit(boolean b) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public List<PinotSegmentOffset> snapshotState() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }
}
