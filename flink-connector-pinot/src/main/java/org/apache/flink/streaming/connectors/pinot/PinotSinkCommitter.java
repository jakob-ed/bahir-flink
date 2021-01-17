/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
