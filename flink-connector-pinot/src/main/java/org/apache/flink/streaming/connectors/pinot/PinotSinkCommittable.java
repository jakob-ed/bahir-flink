package org.apache.flink.streaming.connectors.pinot;

import java.io.File;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkCommittable {
    private final String segmentFilePath;

    public PinotSinkCommittable(String segmentFilePath) {
        checkNotNull(segmentFilePath);
        this.segmentFilePath = segmentFilePath;
    }

    public File getFile() {
        return new File(segmentFilePath);
    }
}
