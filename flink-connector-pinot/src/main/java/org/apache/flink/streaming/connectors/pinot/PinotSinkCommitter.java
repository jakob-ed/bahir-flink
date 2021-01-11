package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkCommitter implements Committer<PinotSinkCommittable> {

    private final String pinotControllerHostPort;

    public PinotSinkCommitter(String pinotControllerHostPort) {
        checkNotNull(pinotControllerHostPort);
        this.pinotControllerHostPort = pinotControllerHostPort;
    }

    @Override
    public List<PinotSinkCommittable> commit(List<PinotSinkCommittable> committables) throws IOException {
        List<PinotSinkCommittable> failedCommits = new ArrayList<>();
        PinotControllerConnection conn = PinotControllerConnection.Factory.createConnection(this.pinotControllerHostPort);
        for (PinotSinkCommittable committable : committables) {
            if (!conn.postSegmentFile(committable.getFile())) {
                failedCommits.add(committable);
            }
        }
        conn.close();
        return failedCommits;
    }

    @Override
    public void close() throws Exception {
        throw new Exception("");
    }
}
