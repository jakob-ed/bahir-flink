package org.apache.flink.streaming.connectors.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkNotNull;

public abstract class FileSystemAdapter implements Serializable {

    protected final String prefix;

    FileSystemAdapter(String prefix) {
        this.prefix = checkNotNull(prefix);
    }

    public abstract Path createTempDirectory() throws IOException;

    public abstract File writeToFile(Path dir, String fileName, List<String> content) throws IOException;
}
