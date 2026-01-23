package com.example.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    private Json(){}

    public static JsonNode parse(String s) {
        try { return MAPPER.readTree(s); }
        catch (Exception e) { throw new RuntimeException("Invalid JSON: " + e.getMessage(), e); }
    }

    public static ObjectNode parseObj(String s) {
        JsonNode n = parse(s);
        if (n instanceof ObjectNode) return (ObjectNode) n;
        throw new RuntimeException("JSON is not an object");
    }

    public static ObjectNode obj() { return MAPPER.createObjectNode(); }

    public static String toString(Object obj) {
        try { return MAPPER.writeValueAsString(obj); }
        catch (JsonProcessingException e) { throw new RuntimeException("JSON serialize error: " + e.getMessage(), e); }
    }

    public static String wrapRaw(String source, String raw) {
        ObjectNode o = obj();
        o.put("_source", source == null ? "unknown" : source);
        o.put("_ingested_at", Instant.now().toString());
        o.put("_raw", raw);
        return o.toString();
    }
}
