package org.apache.flink.streaming.connectors.pinot.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileSystemUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

    /**
     * Writes a list of serialized elements to the temp directory of local filesystem
     * with prefix tempDirPrefix
     *
     * @param elements      List of serialized elements
     * @param tempDirPrefix Prefix used to create the temporary directory
     * @return File containing the written data
     * @throws IOException
     */
    public static File writeToLocalFile(List<String> elements, String tempDirPrefix) throws IOException {
        // Create folder in temp directory for storing data
        Path dir = Files.createTempDirectory(tempDirPrefix);
        LOG.debug("Using path '{}' for storing elements", dir.toAbsolutePath());

        String FILE_NAME = "data.json";
        File dataFile = new File(dir.toAbsolutePath() + "/" + FILE_NAME);
        Files.write(dataFile.toPath(), elements, Charset.defaultCharset());
        LOG.debug("Successfully written data to file {} in directory {}", FILE_NAME, dir.getFileName());

        return dataFile;
    }
}
