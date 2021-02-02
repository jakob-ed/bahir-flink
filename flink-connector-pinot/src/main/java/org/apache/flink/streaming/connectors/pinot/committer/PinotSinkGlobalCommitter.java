package org.apache.flink.streaming.connectors.pinot.committer;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.PinotControllerApi;
import org.apache.flink.streaming.connectors.pinot.PinotSinkUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkGlobalCommitter implements GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkGlobalCommitter.class);

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final SegmentNameGenerator segmentNameGenerator;

    public PinotSinkGlobalCommitter(String pinotControllerHost, String pinotControllerPort, String tableName, SegmentNameGenerator segmentNameGenerator) {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
    }

    @Override
    public List<PinotSinkGlobalCommittable> filterRecoveredCommittables(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        // TODO
        return new ArrayList<>();
    }

    @Override
    public PinotSinkGlobalCommittable combine(List<PinotSinkCommittable> committables) throws IOException {
        List<File> files = new ArrayList<>();
        Long minTimestamp = null;
        Long maxTimestamp = null;

        for (PinotSinkCommittable committable : committables) {
            files.add(committable.getData());
            minTimestamp = PinotSinkUtils.getMin(minTimestamp, committable.getMinTimestamp());
            maxTimestamp = PinotSinkUtils.getMin(maxTimestamp, committable.getMaxTimestamp());
        }

        return new PinotSinkGlobalCommittable(files, minTimestamp, maxTimestamp);
    }

    @Override
    public List<PinotSinkGlobalCommittable> commit(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);
        Schema tableSchema = controllerApi.getSchema(this.tableName);
        TableConfig tableConfig = controllerApi.getTableConfig(this.tableName);

        Long minTimestamp = null;
        Long maxTimestamp = null;

        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            minTimestamp = PinotSinkUtils.getMin(minTimestamp, globalCommittable.getMinTimestamp());
            maxTimestamp = PinotSinkUtils.getMin(maxTimestamp, globalCommittable.getMaxTimestamp());
        }

        List<PinotSinkGlobalCommittable> failedCommits = new ArrayList<>();

        int sequenceId = 0;
        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            try {
                for (File dataFile : globalCommittable.getFiles()) {
                    this.commit(dataFile, sequenceId, minTimestamp, maxTimestamp, tableSchema, tableConfig);
                    sequenceId++;
                }
            } catch (Exception e) {
                Log.error(e.getMessage());
                // TODO: add sequenceId for later recovery
                failedCommits.add(globalCommittable);
            }
        }

        return failedCommits;
    }

    @Override
    public void endOfInput() throws IOException {
    }

    @Override
    public void close() throws Exception {
    }

    private void commit(File segmentData, int sequenceId, Long minTimeValue, Long maxTimeValue, Schema tableSchema, TableConfig tableConfig) throws IOException {
        File segmentFile = Files.createTempDirectory("flink-connector-pinot").toFile();
        LOG.info("Creating segment in " + segmentFile.getAbsolutePath());
        String segmentName = this.segmentNameGenerator.generateSegmentName(sequenceId, minTimeValue, maxTimeValue);

        // Creates a segment with name `segmentName` in `segmentFile`
        Helper.generateSegment(segmentName, segmentData, segmentFile, FileFormat.JSON, null, tableConfig, tableSchema, true);

        // Uploads the recently created segment to the Pinot Controller
        Helper.uploadSegment(segmentFile, this.pinotControllerHost, this.pinotControllerPort);
    }

    static class Helper {

        /**
         * This method was adapted from org.apache.pinot.tools.admin.command.CreateSegmentCommand.java
         *
         * @param dataFile
         * @param outDir
         * @param _format
         * @param recordReaderConfig
         * @param tableConfig
         * @param schema
         * @param _postCreationVerification
         */
        public static void generateSegment(String segmentName, File dataFile, File outDir, FileFormat _format, RecordReaderConfig recordReaderConfig, TableConfig tableConfig, Schema schema, Boolean _postCreationVerification) {
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
            segmentGeneratorConfig.setSegmentName(segmentName);
            segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
            segmentGeneratorConfig.setFormat(_format);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
            segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
            segmentGeneratorConfig.setTableName(tableConfig.getTableName());

            try {
                SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig);
                driver.build();
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
                throw new RuntimeException("Caught exception while generating segment from file: " + dataFile.getPath(), e);
            }
            LOG.info("Successfully created 1 segment from data file: {}", dataFile);
        }

        public static void uploadSegment(File segment, String pinotControllerHost, String pinotControllerPort) throws IOException {
            try {
                UploadSegmentCommand cmd = new UploadSegmentCommand();
                cmd.setControllerHost(pinotControllerHost);
                cmd.setControllerPort(pinotControllerPort);
                cmd.setSegmentDir(segment.getAbsolutePath());
                cmd.execute();
            } catch (Exception e) {
                LOG.info("Could not upload segment {}", segment.toPath(), e);
                throw new IOException(e.getMessage());
            }
        }
    }
}
