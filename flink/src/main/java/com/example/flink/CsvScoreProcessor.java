package com.example.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class CsvScoreProcessor implements RecordProcessor {

    @Override
    public String name() {
        return "csv_score";
    }

    @Override
    public boolean supports(ObjectNode n) {
        return n.hasNonNull("score") && !n.hasNonNull("quiz_score");
    }

    @Override
    public ObjectNode process(ObjectNode n) {
        double score = RecordUtil.toDouble(n, "score");
        score = RecordUtil.clamp(score, 0, 100);

        n.put("performance_index", score);

        long ts = System.currentTimeMillis();
        RecordUtil.putMeta(n, ts);

        return n;
    }
}
