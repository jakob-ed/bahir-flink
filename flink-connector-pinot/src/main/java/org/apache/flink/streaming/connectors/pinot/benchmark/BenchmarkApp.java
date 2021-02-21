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

package org.apache.flink.streaming.connectors.pinot.benchmark;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "benchmark",
        mixinStandardHelpOptions = true,
        subcommands = {
                DataGenerator.class,
                FlinkApp.class
        },
        commandListHeading = "%nCommands:%n%n"
)
public class BenchmarkApp implements Callable<Void> {

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    public static void main(String[] args) {
        System.exit(new CommandLine(new BenchmarkApp()).execute(args));
    }

    @Override
    public Void call() throws CommandLine.ParameterException {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
