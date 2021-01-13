package org.apache.flink.streaming.connectors.pinot.emulator;

import com.spotify.docker.client.exceptions.DockerException;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Serializable;

import static org.apache.flink.streaming.connectors.pinot.emulator.PinotEmulatorManager.getDockerIpAddress;
import static org.apache.flink.streaming.connectors.pinot.emulator.PinotEmulatorManager.getDockerPinotControllerPort;

public class PinotUnitTestBase extends TestLogger implements Serializable {
    @BeforeClass
    public static void launchPinotEmulator() throws Exception {
        // Separated out into separate class so the entire test class to be serializable
        // PinotEmulatorManager.launchDocker();
    }

    @AfterClass
    public static void terminatePinotEmulator() throws DockerException, InterruptedException {
        // PinotEmulatorManager.terminateDocker();
    }

    // ====================================================================================
    // Pinot helpers

    public static PinotHelper getPinotHelper() {
        return new PinotHelper(getPinotControllerHostPort());
    }

    public static String getPinotControllerHostPort() {
        return "localhost:9000"; // getDockerIpAddress() + ":" + getDockerPinotControllerPort();
    }
}
