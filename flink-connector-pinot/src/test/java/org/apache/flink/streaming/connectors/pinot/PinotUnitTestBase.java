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

import com.spotify.docker.client.exceptions.DockerException;
import org.apache.flink.streaming.connectors.pinot.emulator.PinotHelper;
import org.apache.flink.util.TestLogger;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;

import java.io.Serializable;

public class PinotUnitTestBase extends TestLogger implements Serializable {
    @BeforeClass
    public static void launchPinotEmulator() throws Exception {
        // Separated out into separate class so the entire test class to be serializable
        // PinotEmulatorManager.launchDocker();
    }

    @AfterAll
    public static void terminatePinotEmulator() throws DockerException, InterruptedException {
        // PinotEmulatorManager.terminateDocker();
    }

    // ====================================================================================
    // Pinot helpers

    public static PinotHelper getPinotHelper() {
        return new PinotHelper(getPinotControllerHost(), getPinotControllerPort());
    }

    public static String getPinotControllerHost() {
        return "127.0.0.1";
        // getDockerIpAddress() + ":" + getDockerPinotControllerPort();
    }

    public static String getPinotControllerPort() {
        return "9000";
        // getDockerIpAddress() + ":" + getDockerPinotControllerPort();
    }
}
