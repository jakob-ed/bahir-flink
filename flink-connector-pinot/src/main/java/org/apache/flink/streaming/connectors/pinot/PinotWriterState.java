package org.apache.flink.streaming.connectors.pinot;

import com.esotericsoftware.kryo.NotNull;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotWriterState {
    @NotNull
    final Integer latestCommittedSegmentId;

    public PinotWriterState(Integer latestCommittedSegmentId) {
        checkNotNull(latestCommittedSegmentId);
        this.latestCommittedSegmentId = latestCommittedSegmentId;
    }
}
