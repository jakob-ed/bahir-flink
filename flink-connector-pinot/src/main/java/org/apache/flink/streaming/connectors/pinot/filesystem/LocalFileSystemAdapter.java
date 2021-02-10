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
