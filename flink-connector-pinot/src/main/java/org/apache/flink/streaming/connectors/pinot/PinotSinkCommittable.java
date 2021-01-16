package org.apache.flink.streaming.connectors.pinot;

import java.io.File;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkCommittable implements Serializable {
    private final String segmentFilePath;

    public PinotSinkCommittable(String segmentFilePath) {
        checkNotNull(segmentFilePath);
        this.segmentFilePath = segmentFilePath;
    }

    public File getFile() {
        return new File(segmentFilePath);
    }
}
