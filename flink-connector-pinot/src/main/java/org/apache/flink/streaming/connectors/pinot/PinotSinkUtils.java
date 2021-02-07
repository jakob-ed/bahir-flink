package org.apache.flink.streaming.connectors.pinot;

import javax.annotation.Nullable;

public class PinotSinkUtils {
    @Nullable
    public static Long getMin(@Nullable Long first, @Nullable Long second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }

        return Math.min(first, second);
    }

    @Nullable
    public static Long getMax(@Nullable Long first, @Nullable Long second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }

        return Math.max(first, second);
    }
}
