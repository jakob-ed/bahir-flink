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

package org.apache.flink.streaming.connectors.pinot.committer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.Serializable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class PinotSinkGlobalCommittable implements Serializable {
    private final List<File> files;
    private final Long minTimestamp;
    private final Long maxTimestamp;

    public PinotSinkGlobalCommittable(List<File> files, @Nullable Long minTimestamp, @Nullable Long maxTimestamp) {
        this.files = checkNotNull(files);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public List<File> getFiles() {
        return files;
    }

    @Nullable
    public Long getMinTimestamp() {
        return minTimestamp;
    }

    @Nullable
    public Long getMaxTimestamp() {
        return maxTimestamp;
    }
}
