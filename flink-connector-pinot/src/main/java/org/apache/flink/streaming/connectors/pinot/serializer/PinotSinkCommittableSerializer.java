package org.apache.flink.streaming.connectors.pinot.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.PinotSinkCommittable;

import java.io.IOException;

public class PinotSinkCommittableSerializer implements SimpleVersionedSerializer<PinotSinkCommittable> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PinotSinkCommittable pinotSinkCommittable) throws IOException {
        return SerializationUtils.serialize(pinotSinkCommittable);
    }

    @Override
    public PinotSinkCommittable deserialize(int i, byte[] bytes) throws IOException {
        return SerializationUtils.deserialize(bytes);
    }
}
