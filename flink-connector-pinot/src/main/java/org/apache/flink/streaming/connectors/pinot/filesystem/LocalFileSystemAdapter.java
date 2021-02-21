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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class LocalFileSystemAdapter extends FileSystemAdapter {

    public LocalFileSystemAdapter(String prefix) {
        super(prefix);
    }

    @Override
    public Path createTempDirectory() throws IOException {
        return Files.createTempDirectory(this.prefix);
    }

    @Override
    public File writeToFile(Path dir, String fileName, List<String> content) throws IOException {
        File dataFile = new File(dir.toAbsolutePath() + "/" + fileName);
        Files.write(dataFile.toPath(), content, Charset.defaultCharset());
        return dataFile;
    }
}
