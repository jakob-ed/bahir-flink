package org.apache.flink.streaming.connectors.pinot;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkWriter<IN> implements SinkWriter<IN, PinotSinkCommittable, PinotWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkWriter.class);

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
        LOG.info("prepareCommit [subtaskId=" + this.subtaskId + "]: " + this.rows.toString());

        // TODO: store data in dataFile

        File dataFile;
        RecordReaderConfig recordReaderConfig;
        TableConfig tableConfig;
        Schema schema;
        File segmentFile = Helper.generateSegment(dataFile, FileFormat.CSV, recordReaderConfig, tableConfig, schema, true);

        PinotSinkCommittable committable = new PinotSinkCommittable(segmentFile.getPath());
        return Collections.singletonList(committable);
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

    static class Helper {
        /**
         * This method was adapted from org.apache.pinot.tools.admin.command.CreateSegmentCommand.java
         *
         * @param inputFile
         * @param _format
         * @param recordReaderConfig
         * @param tableConfig
         * @param schema
         * @param _postCreationVerification
         */
        public static void generateSegment(File inputFile, FileFormat _format, RecordReaderConfig recordReaderConfig, TableConfig tableConfig, Schema schema, Boolean _postCreationVerification) {
            // TODO: auto-generate segment path
            File outDir;

            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
            segmentGeneratorConfig.setInputFilePath(inputFile.getPath());
            segmentGeneratorConfig.setFormat(_format);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
            segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
            segmentGeneratorConfig.setTableName(tableConfig.getTableName());

            try {
                SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig);
                driver.build();
                String segmentName = driver.getSegmentName();
                File indexDir = new File(outDir, segmentName);
                LOG.info("Successfully created segment: {} at directory: {}", segmentName, indexDir);
                if (_postCreationVerification) {
                    LOG.info("Verifying the segment by loading it");
                    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
                    LOG.info("Successfully loaded segment: {} of size: {} bytes", segmentName,
                            segment.getSegmentSizeBytes());
                    segment.destroy();
                }
            } catch (Exception e) {
                throw new RuntimeException("Caught exception while generating segment from file: " + inputFile.getPath(), e);
            }
        }
    }
}
