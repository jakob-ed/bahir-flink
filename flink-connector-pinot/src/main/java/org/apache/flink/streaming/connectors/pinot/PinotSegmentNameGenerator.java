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

package org.apache.flink.streaming.connectors.pinot;

import org.apache.pinot.core.segment.name.SegmentNameGenerator;

import javax.annotation.Nullable;
import java.io.Serializable;

public class PinotSegmentNameGenerator implements SegmentNameGenerator, Serializable {
    private final String _tableName;
    private final String _segmentNamePostfix;

    public PinotSegmentNameGenerator(String tableName, String segmentNamePostfix) {
        _tableName = tableName;
        _segmentNamePostfix = segmentNamePostfix;
    }

    @Override
    public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
        return JOINER
                .join(_tableName, minTimeValue, maxTimeValue, _segmentNamePostfix, sequenceId >= 0 ? sequenceId : null);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("PinotSegmentNameGenerator: tableName=").append(_tableName);
        if (_segmentNamePostfix != null) {
            stringBuilder.append(", segmentNamePostfix=").append(_segmentNamePostfix);
        }
        return stringBuilder.toString();
    }
}
