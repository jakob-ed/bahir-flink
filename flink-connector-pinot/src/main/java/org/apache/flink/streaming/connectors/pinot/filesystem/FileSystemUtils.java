package org.apache.flink.streaming.connectors.pinot.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class FileSystemUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

    /**
     * Writes a list of serialized elements to the temp directory of local filesystem
     * with prefix tempDirPrefix
     *
     * @param elements  List of serialized elements
     * @param targetDir Directory to create file in
     * @return File containing the written data
     * @throws IOException
     */
    public static File writeToLocalFile(List<String> elements, File targetDir) throws IOException {
        File dataFile = createFileInDir(targetDir);

        Files.write(dataFile.toPath(), elements, Charset.defaultCharset());
        LOG.debug("Successfully written data to file {}", dataFile.getAbsolutePath());

        return dataFile;
    }

    /**
     * Creates file with random name in targetDir.
     *
     * @param targetDir Directory to create file in
     * @return New File
     */
    public static File createFileInDir(File targetDir) {
        String fileName = String.format("%s.json", UUID.randomUUID().toString());
        return new File(targetDir.toString(), fileName);
    }
}
