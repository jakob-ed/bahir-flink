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
