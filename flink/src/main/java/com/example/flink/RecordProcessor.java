package com.example.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public interface RecordProcessor {
    String name();
    boolean supports(ObjectNode n);
    ObjectNode process(ObjectNode n) throws Exception;
}
