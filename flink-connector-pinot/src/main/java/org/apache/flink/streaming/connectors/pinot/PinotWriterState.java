package org.apache.flink.streaming.connectors.pinot;

import com.esotericsoftware.kryo.NotNull;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotWriterState implements Serializable {
    @NotNull
    final Integer latestCommittedSegmentId;

    public PinotWriterState(Integer latestCommittedSegmentId) {
        checkNotNull(latestCommittedSegmentId);
        this.latestCommittedSegmentId = latestCommittedSegmentId;
    }
}
