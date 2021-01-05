package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.Committer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;

public class PinotSinkCommitter implements Committer<PinotSegmentConfig> {
    @Override
    public List<PinotSegmentConfig> commit(List<PinotSegmentConfig> list) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }
}
