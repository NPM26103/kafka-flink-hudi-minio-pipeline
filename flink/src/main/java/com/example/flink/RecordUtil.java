package com.example.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

public final class RecordUtil {

    private RecordUtil() {}

    public static long toLong(ObjectNode n, String k) {
        if (n.get(k).isNumber()) return n.get(k).asLong();
        return Long.parseLong(n.get(k).asText().trim());
    }

    public static double toDouble(ObjectNode n, String k) {
        if (n.get(k).isNumber()) return n.get(k).asDouble();
        return Double.parseDouble(n.get(k).asText().trim());
    }

    public static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    public static void putMeta(ObjectNode n, long ts) {
        n.put("_pipeline", "kafka->flink->kafka");
        n.put("_processed_at", Instant.ofEpochMilli(ts).toString());
        n.put("processed_ts_ms", ts);
        n.put("ts_ms", ts);
    }
}
