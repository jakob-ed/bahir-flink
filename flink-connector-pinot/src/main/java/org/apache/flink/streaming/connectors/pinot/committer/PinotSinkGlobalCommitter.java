/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.committer;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.PinotControllerApi;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
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
import org.apache.pinot.tools.admin.command.UploadSegmentCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Global committer takes committables from {@link org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter},
 * generates segments and pushed them to the Pinot controller.
 * Note: We use a custom multithreading approach to parallelize the segment creation and upload to
 * overcome the performance limitations resulting from using a {@link GlobalCommitter} always
 * running at a parallelism of 1.
 */
public class PinotSinkGlobalCommitter implements GlobalCommitter<PinotSinkCommittable, PinotSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkGlobalCommitter.class);

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final SegmentNameGenerator segmentNameGenerator;
    private final String tempDirPrefix;
    private final FileSystemAdapter fsAdapter;
    private final String timeColumnName;
    private final TimeUnit segmentTimeUnit;

    /**
     * @param pinotControllerHost  Host of the Pinot controller
     * @param pinotControllerPort  Port of the Pinot controller
     * @param tableName            Target table's name
     * @param segmentNameGenerator Pinot segment name generator
     * @param fsAdapter            Adapter for interacting with the shared file system
     * @param timeColumnName       Name of the column containing the timestamp
     * @param segmentTimeUnit      Unit of the time column
     */
    public PinotSinkGlobalCommitter(String pinotControllerHost, String pinotControllerPort, String tableName, SegmentNameGenerator segmentNameGenerator, String tempDirPrefix, FileSystemAdapter fsAdapter, String timeColumnName, TimeUnit segmentTimeUnit) {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
        this.tempDirPrefix = checkNotNull(tempDirPrefix);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.timeColumnName = checkNotNull(timeColumnName);
        this.segmentTimeUnit = checkNotNull(segmentTimeUnit);
    }

    /**
     * Identifies global committables that need to be re-committed from a list of recovered committables.
     *
     * @param globalCommittables List of global committables that are checked for required re-commit
     * @return List of global committable that need to be re-committed
     * @throws IOException
     */
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

    /**
     * Combines multiple {@link PinotSinkCommittable}s into one {@link PinotSinkGlobalCommittable}
     * by finding the minimum and maximum timestamps from the provided {@link PinotSinkCommittable}s.
     *
     * @param committables Committables created by {@link org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter}
     * @return Global committer committable
     */
    @Override
    public PinotSinkGlobalCommittable combine(List<PinotSinkCommittable> committables) {
        List<String> dataFilePaths = new ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        // Extract all data file paths and the overall minimum and maximum timestamps
        // from all committables
        for (PinotSinkCommittable committable : committables) {
            dataFilePaths.add(committable.getDataFilePath());
            minTimestamp = Long.min(minTimestamp, committable.getMinTimestamp());
            maxTimestamp = Long.max(maxTimestamp, committable.getMaxTimestamp());
        }

        LOG.info("Combined {} committables into one global committable", committables.size());
        return new PinotSinkGlobalCommittable(dataFilePaths, minTimestamp, maxTimestamp);
    }

    /**
     * Copies data files from shared filesystem to the local filesystem, generates segments with names
     * according to the segment naming schema and finally pushes the segments to the Pinot cluster.
     * Before pushing a segment it is checked whether there already exists a segment with that name
     * in the Pinot cluster by calling the Pinot controller. In case there is one, it gets deleted.
     *
     * @param globalCommittables List of global committables
     * @return Global committables whose commit failed
     * @throws IOException
     */
    @Override
    public List<PinotSinkGlobalCommittable> commit(List<PinotSinkGlobalCommittable> globalCommittables) throws IOException {
        // Retrieve the Pinot table schema and the Pinot table config from the Pinot controller
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);
        Schema tableSchema = controllerApi.getSchema(this.tableName);
        TableConfig tableConfig = controllerApi.getTableConfig(this.tableName);

        // List of failed global committables that can be retried later on
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

            // We use a thread pool in order to parallelize the segment creation and segment upload
            ExecutorService pool = Executors.newCachedThreadPool();
            Set<Future<Boolean>> resultFutures = new HashSet<>();

            // Commit all segments in globalCommittable
            int sequenceId = 0;
            for (String dataFilePath : globalCommittable.getDataFilePaths()) {
                // Get segment names with increasing sequenceIds
                String segmentName = this.getSegmentName(globalCommittable, sequenceId++);
                // Segment committer handling the whole commit process for a single segment
                Callable<Boolean> segmentCommitter = new SegmentCommitter(
                        this.pinotControllerHost, this.pinotControllerPort, this.tempDirPrefix,
                        this.fsAdapter, dataFilePath, segmentName, tableSchema, tableConfig,
                        this.timeColumnName, this.segmentTimeUnit
                );
                // Submits the segment committer to the thread pool
                resultFutures.add(pool.submit(segmentCommitter));
            }

            try {
                for (Future<Boolean> wasSuccessful : resultFutures) {
                    // In case any of the segment commits wasn't successful we mark the whole
                    // globalCommittable as failed
                    if (!wasSuccessful.get()) {
                        failedCommits.add(globalCommittable);
                        // Once any of the commits failed, we do not need to check the remaining
                        // ones, as we try to commit the globalCommittable next time
                        break;
                    }
                }
            } catch (Exception e) {
                // In case of an exception mark the whole globalCommittable as failed
                failedCommits.add(globalCommittable);
                LOG.error(e.getMessage());
            }
        }

        // Return failed commits so that they can be retried later on
        return failedCommits;
    }

    /**
     * Empty method.
     */
    @Override
    public void endOfInput() {
    }

    /**
     * Empty method, as we do not open any connections.
     */
    @Override
    public void close() {
    }

    /**
     * Helper method for generating segment names using the segment name generator.
     *
     * @param globalCommittable Global committable the segment name shall be generated from
     * @param sequenceId        Incrementing counter
     * @return generated segment name
     */
    private String getSegmentName(PinotSinkGlobalCommittable globalCommittable, int sequenceId) {
        return this.segmentNameGenerator.generateSegmentName(sequenceId, globalCommittable.getMinTimestamp(), globalCommittable.getMaxTimestamp());
    }

    /**
     * Evaluates the status of already uploaded segments by requesting segment metadata from the
     * Pinot controller.
     *
     * @param globalCommittable Global committable whose commit status gets evaluated
     * @return Commit status
     * @throws IOException
     */
    private CommitStatus getCommitStatus(PinotSinkGlobalCommittable globalCommittable) throws IOException {
        PinotControllerApi controllerApi = new PinotControllerApi(this.pinotControllerHost, this.pinotControllerPort);

        List<String> existingSegmentNames = new ArrayList<>();
        List<String> missingSegmentNames = new ArrayList<>();

        // For all segment names that will be used to submit new segments, check whether the segment
        // name already exists for the target table
        for (int sequenceId = 0; sequenceId < globalCommittable.getDataFilePaths().size(); sequenceId++) {
            String segmentName = this.getSegmentName(globalCommittable, sequenceId);
            if (controllerApi.tableHasSegment(this.tableName, segmentName)) {
                // Segment name already exists
                existingSegmentNames.add(segmentName);
            } else {
                // Segment name does not exist yet
                missingSegmentNames.add(segmentName);
            }
        }

        return new CommitStatus(existingSegmentNames, missingSegmentNames);
    }

    /**
     * Wrapper for existing and missing segments in the Pinot cluster.
     */
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

    /**
     * Helper class for committing a single segment. Downloads a data file from the shared filesystem,
     * generates a segment from the data file and uploads segment to the Pinot controller.
     */
    static class SegmentCommitter implements Callable<Boolean> {

        private static final Logger LOG = LoggerFactory.getLogger(SegmentCommitter.class);

        final String pinotControllerHost;
        final String pinotControllerPort;
        final String tempDirPrefix;
        final FileSystemAdapter fsAdapter;
        final String dataFilePath;
        final String segmentName;
        final Schema tableSchema;
        final TableConfig tableConfig;
        final String timeColumnName;
        final TimeUnit segmentTimeUnit;

        /**
         * @param pinotControllerHost Host of the Pinot controller
         * @param pinotControllerPort Port of the Pinot controller
         * @param fsAdapter           Filesystem adapter used to load data files from the shared file system
         * @param dataFilePath        Data file to load from the shared file system
         * @param segmentName         Name of the segment to create and commit
         * @param tableSchema         Pinot table schema
         * @param tableConfig         Pinot table config
         * @param timeColumnName      Name of the column containing the timestamp
         * @param segmentTimeUnit     Unit of the time column
         */
        public SegmentCommitter(String pinotControllerHost, String pinotControllerPort, String tempDirPrefix, FileSystemAdapter fsAdapter, String dataFilePath, String segmentName, Schema tableSchema, TableConfig tableConfig, String timeColumnName, TimeUnit segmentTimeUnit) {
            this.pinotControllerHost = pinotControllerHost;
            this.pinotControllerPort = pinotControllerPort;
            this.tempDirPrefix = tempDirPrefix;
            this.fsAdapter = fsAdapter;
            this.dataFilePath = dataFilePath;
            this.segmentName = segmentName;
            this.tableSchema = tableSchema;
            this.tableConfig = tableConfig;
            this.timeColumnName = timeColumnName;
            this.segmentTimeUnit = segmentTimeUnit;
        }

        /**
         * Downloads a segment from the shared file system via {@code fsAdapter}, generates a segment
         * and finally uploads the segment to the Pinot controller
         *
         * @return True if the commit succeeded
         */
        @Override
        public Boolean call() {
            try {
                // Download data file from the shared filesystem
                LOG.info("Downloading data file {} from shared file system...", dataFilePath);
                File segmentData = fsAdapter.copyToLocalFile(dataFilePath);
                LOG.info("Successfully downloaded data file {} from shared file system", dataFilePath);

                File segmentFile = Files.createTempDirectory(this.tempDirPrefix).toFile();
                LOG.info("Creating segment in " + segmentFile.getAbsolutePath());

                // Creates a segment with name `segmentName` in `segmentFile`
                this.generateSegment(segmentData, segmentFile, true);

                // Uploads the recently created segment to the Pinot controller
                this.uploadSegment(segmentFile);

                // Commit successful
                return true;
            } catch (IOException e) {
                LOG.error(e.getMessage());

                // Commit failed
                return false;
            }
        }

        /**
         * Creates a segment from the given parameters.
         * This method was adapted from {@link org.apache.pinot.tools.admin.command.CreateSegmentCommand}.
         *
         * @param dataFile                  File containing the JSON data
         * @param outDir                    Segment target path
         * @param _postCreationVerification Verify segment after generation
         */
        public void generateSegment(File dataFile, File outDir, Boolean _postCreationVerification) {
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, tableSchema);
            segmentGeneratorConfig.setSegmentName(segmentName);
            segmentGeneratorConfig.setSegmentTimeUnit(segmentTimeUnit);
            segmentGeneratorConfig.setTimeColumnName(timeColumnName);
            segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
            segmentGeneratorConfig.setFormat(FileFormat.JSON);
            segmentGeneratorConfig.setOutDir(outDir.getPath());
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
                LOG.error(e.getMessage());
                throw new RuntimeException("Caught exception while generating segment from file: " + dataFile.getPath());
            }
            LOG.info("Successfully created 1 segment from data file: {}", dataFile);
        }

        /**
         * Uploads a segment using the Pinot admin tool.
         *
         * @param segmentFile File containing the segment to upload
         * @throws IOException
         */
        public void uploadSegment(File segmentFile) throws IOException {
            try {
                UploadSegmentCommand cmd = new UploadSegmentCommand();
                cmd.setControllerHost(this.pinotControllerHost);
                cmd.setControllerPort(this.pinotControllerPort);
                cmd.setSegmentDir(segmentFile.getAbsolutePath());
                cmd.execute();
            } catch (Exception e) {
                LOG.info("Could not upload segment {}", segmentFile.getAbsolutePath(), e);
                throw new IOException(e.getMessage());
            }
        }
    }
}
