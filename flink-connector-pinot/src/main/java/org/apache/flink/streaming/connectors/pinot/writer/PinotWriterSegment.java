package org.apache.flink.streaming.connectors.pinot.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.connectors.pinot.PinotSinkUtils;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkCommittable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

public class PinotWriterSegment<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotWriterSegment.class);

    private final int maxRowsPerSegment;

    private List<IN> rows;
    private Long minTimestamp = null;
    private Long maxTimestamp = null;

    protected PinotWriterSegment(int maxRowsPerSegment) {
        checkArgument(maxRowsPerSegment > 0L);
        this.maxRowsPerSegment = maxRowsPerSegment;
        this.rows = new ArrayList<>();
    }

    public void write(IN element, Long timestmap) {
        if (!this.acceptsElements()) {
            throw new IllegalStateException("This PinotSegmentWriter does not accept any elements anymore.");
        }
        this.rows.add(element);
        this.minTimestamp = PinotSinkUtils.getMin(this.minTimestamp, timestmap);
        this.maxTimestamp = PinotSinkUtils.getMax(this.maxTimestamp, timestmap);
    }

    public PinotSinkCommittable prepareCommit() throws IOException {
        File dataFile = this.flush();
        return new PinotSinkCommittable(dataFile, this.minTimestamp, this.maxTimestamp);
    }

    private File flush() throws IOException {
        // Create folder in temp directory for storing data
        Path dir = Files.createTempDirectory("flink-connector-pinot");
        LOG.info("Using path '{}' for storing committables", dir.toAbsolutePath());

        // Stores row items in JSON format on disk
        File dataFile = new File(dir.toAbsolutePath() + "/data.json");
        List<String> json = rows.stream()
                .map(b -> {
                    try {
                        JsonNode jsonNode = JsonUtils.objectToJsonNode(b);
                        return jsonNode;
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage());
                    }
                })
                .map(JsonNode::toString)
                .collect(Collectors.toList());
        Files.write(dataFile.toPath(), json, Charset.defaultCharset());
        LOG.info("Successfully written data to {}", dataFile.getAbsolutePath());

        return dataFile;
    }

    public boolean acceptsElements() {
        return this.rows.size() < this.maxRowsPerSegment;
    }
}
