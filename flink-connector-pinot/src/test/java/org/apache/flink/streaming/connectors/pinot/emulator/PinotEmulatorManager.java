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

package org.apache.flink.streaming.connectors.pinot.emulator;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PinotEmulatorManager {
    private static final Logger LOG = LoggerFactory.getLogger(PinotEmulatorManager.class);

    private static DockerClient docker;

    public static final int INTERNAL_PINOT_CONTROLLER_PORT = 9000;
    public static final int INTERNAL_PINOT_BROKER_PORT = 8000;
    public static final String DOCKER_IMAGE_NAME = "apachepinot/pinot:latest";

    public static final String UNITTEST_PROJECT_ID = "running-from-junit-for-flink";
    private static final String CONTAINER_NAME_JUNIT =
            (DOCKER_IMAGE_NAME + "_" + UNITTEST_PROJECT_ID).replaceAll("[^a-zA-Z0-9_]", "_");

    // Timeout for waiting for a success response after container start up
    private static final long MAX_RETRY_TIMEOUT = 60000; // Milliseconds

    /**
     * Launches a clean Pinot docker container.
     *
     * @return Container ports resulting from port mappings
     * @throws InterruptedException
     */
    public static ContainerPorts launchDocker() throws InterruptedException {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .build();
        docker = DockerClientImpl.getInstance(config, httpClient);
        terminateAndDiscardAnyExistingContainers(true);

        LOG.info("");
        LOG.info("/===========================================");
        LOG.info("| Pinot Emulator");

        InspectContainerResponse containerInfo;
        String id;

        try {
            docker.inspectImageCmd(DOCKER_IMAGE_NAME).exec();
        } catch (NotFoundException e) {
            // No such image so we must download it first.
            LOG.info("| - Getting docker image \"{}\"", DOCKER_IMAGE_NAME);
            docker.pullImageCmd(DOCKER_IMAGE_NAME).exec(new PullImageResultCallback() {
                @Override
                public void onNext(PullResponseItem item) {
                    LOG.info("| - Downloading > {} : {}", item.getId(), item.getProgress());
                }
            });
        }

        // No such container. Good, we create one!
        LOG.info("| - Creating new container");

        // Bind container ports to host ports
        List<ExposedPort> exposedPorts = Arrays.asList(
                new ExposedPort(INTERNAL_PINOT_CONTROLLER_PORT),
                new ExposedPort(INTERNAL_PINOT_BROKER_PORT)
        );

        final List<PortBinding> portBindings = exposedPorts.stream()
                .map(exposedPort ->
                        // Assigns exposedPort to random port
                        new PortBinding(new Ports.Binding("127.0.0.1", null), exposedPort)
                )
                .collect(Collectors.toList());

        HostConfig hostConfig = new HostConfig()
                .withPortBindings(portBindings);

        final CreateContainerResponse creation = docker.createContainerCmd(DOCKER_IMAGE_NAME)
                .withHostConfig(hostConfig)
                .withExposedPorts(exposedPorts)
                .withName(CONTAINER_NAME_JUNIT)
                .withCmd("QuickStart", "-type", "batch")
                .exec();
        id = creation.getId();

        containerInfo = docker.inspectContainerCmd(id).exec();

        if (!containerInfo.getState().getRunning()) {
            LOG.info("| - Starting it up ....");
            docker.startContainerCmd(id).exec();
            Thread.sleep(1000);
        }

        containerInfo = docker.inspectContainerCmd(id).exec();

        String dockerIpAddress = "127.0.0.1";

        Ports ports = containerInfo.getNetworkSettings().getPorts();

        assertNotNull("Unable to retrieve the ports where to connect to the emulators", ports);
        assertEquals("We expect 1 port to be mapped", 6, ports.getBindings().size());

        String pinotControllerPort = getPort(ports, INTERNAL_PINOT_CONTROLLER_PORT, "PinotController");
        String pinotBrokerPort = getPort(ports, INTERNAL_PINOT_BROKER_PORT, "PinotBroker");

        LOG.info("| Waiting for the emulators to be running");

        // Pinot exposes an "Ok" at the root url when running.
        if (!waitForOkStatus("PinotController", dockerIpAddress, pinotControllerPort)) {
            // Oops, we did not get an "Ok" within 10 seconds
            startHasFailedKillEverything();
            System.exit(1);
        }
        LOG.info("\\===========================================");
        LOG.info("");

        return new ContainerPorts(dockerIpAddress, pinotControllerPort, pinotBrokerPort);
    }

    /**
     * Initiates container kill process.
     */
    private static void startHasFailedKillEverything() {
        LOG.error("|");
        LOG.error("| ==================== ");
        LOG.error("| YOUR TESTS WILL FAIL ");
        LOG.error("| ==================== ");
        LOG.error("|");

        // Kill this container
        terminateAndDiscardAnyExistingContainers(false);
    }

    /**
     * Blocks until the Pinot container has successfully started.
     *
     * @param label           Label for the port to test against
     * @param dockerIpAddress The host the container is reachable at
     * @param port            The port to send the request to
     * @return True if a response was received within {@code MAX_RETRY_TIMEOUT}
     */
    private static boolean waitForOkStatus(String label, String dockerIpAddress, String port) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                URL url = new URL("http://" + dockerIpAddress + ":" + port + "/help");
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                con.setConnectTimeout(50);
                con.setReadTimeout(50);

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();

                if (content.toString().length() > 0) {
                    LOG.info("| - {} Emulator is running at {}:{}", label, dockerIpAddress, port);
                    return true;
                }
            } catch (IOException e) {
                long now = System.currentTimeMillis();
                if (now - start > MAX_RETRY_TIMEOUT) {
                    System.out.println(
                            "| - Pinot Emulator at " + dockerIpAddress + ":" + port + " FAILED to return an Ok status within " + MAX_RETRY_TIMEOUT + " ms "
                    );
                    return false;
                }
                try {
                    Thread.sleep(100); // Sleep a very short time
                } catch (InterruptedException e1) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Extracts a public port given its container internal port.
     *
     * @param ports           All container ports
     * @param internalTCPPort Internal port to get the external port for
     * @param label           The port's label
     * @return The external port
     */
    private static String getPort(Ports ports, int internalTCPPort, String label) {
        Optional<Map.Entry<ExposedPort, Ports.Binding[]>> portMapping = ports.getBindings().entrySet()
                .stream()
                .filter(exposedPortEntry -> exposedPortEntry.getKey().getPort() == internalTCPPort)
                .findFirst();

        if (!portMapping.isPresent() || portMapping.get().getValue().length == 0) {
            LOG.info("| {} Emulator --> NOTHING CONNECTED TO {}/tcp", label, internalTCPPort);
            return null;
        }

        return portMapping.get().getValue()[0].getHostPortSpec();
    }

    /**
     * Stops and removes the test container.
     *
     * @param warnAboutExisting Determines whether to show warn statements
     */
    private static void terminateAndDiscardAnyExistingContainers(boolean warnAboutExisting) {
        InspectContainerResponse containerInfo;
        try {
            containerInfo = docker.inspectContainerCmd(CONTAINER_NAME_JUNIT).exec();
            // Already have this container running.

            assertNotNull(
                    "We should either get a containerInfo or we get an exception", containerInfo);

            LOG.info("");
            LOG.info("/===========================================");
            if (warnAboutExisting) {
                LOG.warn("|    >>> FOUND OLD EMULATOR INSTANCE RUNNING <<< ");
                LOG.warn("| Destroying that one to keep tests running smoothly.");
            }
            LOG.info("| Cleanup of Pinot Emulator. Log output of container: ");

            if (LOG.isInfoEnabled()) {
                try {
                    docker.logContainerCmd(containerInfo.getId())
                            .withStdOut(true)
                            .withStdErr(true)
                            .withTimestamps(true)
                            .exec(new LogContainerResultCallback() {
                                @Override
                                public void onNext(Frame item) {
                                    LOG.info("| > {}", item.toString());
                                }
                            }).awaitCompletion();
                } catch (InterruptedException ignored) {

                }
            }

            String id = containerInfo.getId();
            // Kill container
            if (containerInfo.getState().getRunning()) {
                docker.killContainerCmd(id).exec();
                LOG.info("| - Killed");
            }

            // Remove container
            docker.removeContainerCmd(id).exec();

            LOG.info("| - Removed");
            LOG.info("\\===========================================");
            LOG.info("");

        } catch (NotFoundException cnfe) {
            // No such container. Good !
        }
    }

    /**
     * Kills a possibly running test container and finally closes the docker client.
     *
     * @throws IOException
     */
    public static void terminateDocker() throws IOException {
        terminateAndDiscardAnyExistingContainers(false);

        // Close the docker client
        docker.close();
    }

    /**
     * Class used to store the Pinot host and ports.
     */
    public static class ContainerPorts {
        final String dockerIpAddress;
        final String pinotControllerPort;
        final String pinotBrokerPort;

        public ContainerPorts(String dockerIpAddress, String pinotControllerPort, String pinotBrokerPort) {
            this.dockerIpAddress = dockerIpAddress;
            this.pinotControllerPort = pinotControllerPort;
            this.pinotBrokerPort = pinotBrokerPort;
        }

        public String getDockerIpAddress() {
            return dockerIpAddress;
        }

        public String getPinotControllerPort() {
            return pinotControllerPort;
        }

        public String getPinotBrokerPort() {
            return pinotBrokerPort;
        }
    }
}
