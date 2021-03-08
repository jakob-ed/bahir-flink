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

package org.apache.flink.streaming.connectors.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * Defines the interaction with a shared filesystem. The shared filesystem must be accessible from all
 * nodes within the cluster than run a partition of the {@link org.apache.flink.streaming.connectors.pinot.PinotSink}.
 */
public abstract class FileSystemAdapter implements Serializable {

    /**
     * Copies a file from the local file system to the shared filesystem.
     *
     * @param file Input file to copy
     * @return Path identifying the remote file
     * @throws IOException
     */
    public abstract String copyToSharedFileSystem(File file) throws IOException;


    /**
     * Copies a file from the remote filesystem to the local filesystem.
     *
     * @param path Path identifying the remote file
     * @return File on local filesystem
     * @throws IOException
     */
    public abstract File copyToLocalFile(String path) throws IOException;
}
