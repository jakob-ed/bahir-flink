package org.apache.flink.streaming.connectors.pinot.serializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.PinotSinkCommittable;
import org.apache.flink.streaming.connectors.pinot.PinotWriterState;

import java.io.IOException;

public class PinotWriterStateSerializer implements SimpleVersionedSerializer<PinotWriterState> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(PinotWriterState pinotWriterState) throws IOException {
        return new byte[0];
    }

    @Override
    public PinotWriterState deserialize(int i, byte[] bytes) throws IOException {
        // TODO
        return new PinotWriterState(0);
    }
}
