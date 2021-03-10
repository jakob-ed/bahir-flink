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

package org.apache.flink.streaming.connectors.pinot.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommittable;

/**
 * Serializer for {@link PinotSinkGlobalCommittable}
 */
public class PinotSinkGlobalCommittableSerializer implements SimpleVersionedSerializer<PinotSinkGlobalCommittable> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PinotSinkGlobalCommittable pinotSinkGlobalCommittable) {
        return SerializationUtils.serialize(pinotSinkGlobalCommittable);
    }

    @Override
    public PinotSinkGlobalCommittable deserialize(int i, byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }
}
