package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkCommitter implements Committer<PinotSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkCommitter.class);

    private final String pinotControllerHost;
    private final String pinotControllerPort;

    public PinotSinkCommitter(String pinotControllerHost, String pinotControllerPort) {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
    }

    @Override
    public List<PinotSinkCommittable> commit(List<PinotSinkCommittable> committables) throws IOException {
        List<PinotSinkCommittable> failedCommits = new ArrayList<>();
        for (PinotSinkCommittable committable : committables) {
            try {
                Helper.uploadSegment(this.pinotControllerHost, this.pinotControllerPort, committable.getFile());
                LOG.info("Successfully uploaded segment at {}", committable.getFile());
            } catch (Exception e) {
                LOG.info("Could not upload segment {}", committable.getFile().toPath(), e);
                failedCommits.add(committable);
            }
        }
        return failedCommits;
    }

    @Override
    public void close() throws Exception {

    }

    static class Helper {

        public static void uploadSegment(String controllerHost, String controllerPort, File segmentDir) throws Exception {
            UploadSegmentCommand cmd = new UploadSegmentCommand();
            cmd.setControllerHost(controllerHost);
            cmd.setControllerPort(controllerPort);
            cmd.setSegmentDir(segmentDir.getAbsolutePath());

            cmd.execute();
        }
    }
}
