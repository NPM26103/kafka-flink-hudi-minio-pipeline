package com.example.flink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;

public class ProcessorChain extends ProcessFunction<String, String> {

    private final OutputTag<String> dlqTag;
    private final List<RecordProcessor> processors;

    public ProcessorChain(OutputTag<String> dlqTag, List<RecordProcessor> processors) {
        this.dlqTag = dlqTag;
        this.processors = processors;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        final ObjectNode n;
        try {
            n = FlinkJson.parseObj(value);
        } catch (Exception e) {
            ctx.output(dlqTag, buildDlq(value, "INVALID_JSON", e.getMessage(), "parse"));
            return;
        }

        for (RecordProcessor p : processors) {
            if (!p.supports(n)) continue;

            try {
                ObjectNode outNode = p.process(n);
                out.collect(outNode.toString());
            } catch (Exception e) {
                ctx.output(dlqTag, buildDlq(n.toString(), "VALIDATION_ERROR", e.getMessage(), p.name()));
            }
            return;
        }

        ctx.output(dlqTag, buildDlq(n.toString(), "NO_PROCESSOR", "No processor matched schema", "chain"));
    }

    private static String buildDlq(String raw, String code, String msg, String stage) {
        ObjectNode err = FlinkJson.obj();
        err.put("_error_code", code);
        err.put("_error_message", msg);
        err.put("_stage", stage);
        err.put("_raw", raw);
        err.put("_pipeline", "kafka->flink->kafka");
        err.put("_failed_at", Instant.now().toString());
        return err.toString();
    }
}
