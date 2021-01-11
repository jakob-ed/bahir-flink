package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, PinotWriterState> {

    private Integer subtaskId;
    private Integer latestCommittedId = -1;

    private List<IN> rows;

    public PinotSinkWriter(Integer subtaskId) {
        checkNotNull(subtaskId);
        this.subtaskId = subtaskId;

        this.rows = new ArrayList<>();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        // TODO: we need to move this to disk, to prevent a large memory footprint
        this.rows.add(element);
    }

    @Override
    public List<PinotSinkCommittable> prepareCommit(boolean b) throws IOException {
        // TODO
        // Aggregate individual rows into one segment
        // Return a segment config
        throw new IOException("");
    }

    public void initializeState(List<PinotWriterState> states) {
        this.latestCommittedId = states.stream()
                .mapToInt(state -> state.latestCommittedSegmentId)
                .max().orElseGet(() -> -1);
    }

    @Override
    public List<PinotWriterState> snapshotState() throws IOException {
        PinotWriterState state = new PinotWriterState(this.latestCommittedId);
        return Collections.singletonList(state);
    }

    @Override
    public void close() throws InterruptedException {
        // TODO
    }
}
