package org.apache.flink.streaming.connectors.pinot;

import javax.annotation.Nullable;

public class PinotSinkUtils {
    @Nullable
    public static Long getMin(@Nullable Long first, @Nullable Long second) {
        boolean firstIsNull = first == null;
        boolean secondIsNull = second == null;

        if (firstIsNull) {
            return second;
        }
        if (secondIsNull) {
            return first;
        }

        return Math.min(first, second);
    }

    @Nullable
    public static Long getMax(@Nullable Long first, @Nullable Long second) {
        boolean firstIsNull = first == null;
        boolean secondIsNull = second == null;

        if (firstIsNull) {
            return second;
        }
        if (secondIsNull) {
            return first;
        }

        return Math.max(first, second);
    }
}
