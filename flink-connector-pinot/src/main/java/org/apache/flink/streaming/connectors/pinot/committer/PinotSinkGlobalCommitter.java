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
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);
        List<PinotSinkGlobalCommittable> committablesToRetry = new ArrayList<>();

        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            CommitStatus commitStatus = this.getCommitStatus(globalCommittable);

            if (commitStatus.getMissingSegmentNames().isEmpty()) {
                // All segments were already committed. Thus, we do not need to retry the commit.
                continue;
            }

            for (String existingSegment : commitStatus.getExistingSegmentNames()) {
                // Some but not all segments were already committed. As we cannot assure the data
                // files containing the same data as originally when recovering from failure,
                // we delete the already committed segments in order to recommit them later on.
                controllerApi.deleteSegment(tableName, existingSegment);
            }
            committablesToRetry.add(globalCommittable);
        }

        return committablesToRetry;
    }

    @Override
    public PinotSinkGlobalCommittable combine(List<PinotSinkCommittable> committables) {
        List<File> files = new ArrayList<>();
        Long minTimestamp = null;
        Long maxTimestamp = null;

        for (PinotSinkCommittable committable : committables) {
            files.add(committable.getDataFile());
            minTimestamp = PinotSinkUtils.getMin(minTimestamp, committable.getMinTimestamp());
            maxTimestamp = PinotSinkUtils.getMax(maxTimestamp, committable.getMaxTimestamp());
        }

        return new PinotSinkGlobalCommittable(files, minTimestamp, maxTimestamp);
    }

    @Override
    public List<PinotSinkGlobalCommittable> commit(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);
        Schema tableSchema = controllerApi.getSchema(this.tableName);
        TableConfig tableConfig = controllerApi.getTableConfig(this.tableName);

        List<PinotSinkGlobalCommittable> failedCommits = new ArrayList<>();

        for (PinotSinkGlobalCommittable globalCommittable : globalCommittables) {
            // Make sure to remove all previously committed segments in globalCommittable
            // when recovering from failure
            CommitStatus commitStatus = this.getCommitStatus(globalCommittable);
            for (String existingSegment : commitStatus.getExistingSegmentNames()) {
                // Some but not all segments were already committed. As we cannot assure the data
                // files containing the same data as originally when recovering from failure,
                // we delete the already committed segments in order to recommit them later on.
                controllerApi.deleteSegment(tableName, existingSegment);
            }

            // Commit all segments in globalCommittable
            int sequenceId = 0;
            try {
                for (File dataFile : globalCommittable.getDataFiles()) {
                    String segmentName = this.getSegmentName(globalCommittable, sequenceId);
                    this.commit(dataFile, segmentName, tableSchema, tableConfig);
                    sequenceId++;
                }
            } catch (Exception e) {
                Log.error(e.getMessage());
                failedCommits.add(globalCommittable);
            }
        }

        return failedCommits;
    }

    @Override
    public void endOfInput() {
    }

    @Override
    public void close() {
    }

    private String getSegmentName(PinotSinkGlobalCommittable globalCommittable, int sequenceId) {
        return this.segmentNameGenerator.generateSegmentName(sequenceId, globalCommittable.getMinTimestamp(), globalCommittable.getMaxTimestamp());
    }

    private CommitStatus getCommitStatus(PinotSinkGlobalCommittable globalCommittable) throws IOException {
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);

        List<String> existingSegmentNames = new ArrayList<>();
        List<String> missingSegmentNames = new ArrayList<>();

        for (int sequenceId = 0; sequenceId < globalCommittable.getDataFiles().size(); sequenceId++) {
            String segmentName = this.getSegmentName(globalCommittable, sequenceId);
            if (controllerApi.tableHasSegment(this.tableName, segmentName)) {
                existingSegmentNames.add(segmentName);
            } else {
                missingSegmentNames.add(segmentName);
            }
        }

        return new CommitStatus(existingSegmentNames, missingSegmentNames);
    }

    private void commit(File segmentData, String segmentName, Schema tableSchema, TableConfig tableConfig) throws IOException {
        File segmentFile = Files.createTempDirectory("flink-connector-pinot").toFile();
        LOG.info("Creating segment in " + segmentFile.getAbsolutePath());

        // Creates a segment with name `segmentName` in `segmentFile`
        Helper.generateSegment(segmentName, segmentData, segmentFile, FileFormat.JSON, null, tableConfig, tableSchema, true);

        // Uploads the recently created segment to the Pinot Controller
        Helper.uploadSegment(segmentFile, this.pinotControllerHost, this.pinotControllerPort);
    }

    static class CommitStatus {
        private final List<String> existingSegmentNames;
        private final List<String> missingSegmentNames;

        public CommitStatus(List<String> existingSegmentNames, List<String> missingSegmentNames) {
            this.existingSegmentNames = existingSegmentNames;
            this.missingSegmentNames = missingSegmentNames;
        }

        public List<String> getExistingSegmentNames() {
            return existingSegmentNames;
        }

        public List<String> getMissingSegmentNames() {
            return missingSegmentNames;
        }
    }

    static class Helper {

        /**
         * This method was adapted from org.apache.pinot.tools.admin.command.CreateSegmentCommand.java
         *
         * @param segmentName
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
                LOG.info("Successfully created segment: {} in directory: {}", segmentName, indexDir);
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
