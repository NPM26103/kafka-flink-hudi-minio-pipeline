package com.example.ingest.sources;

public interface IngestSource {
    enum Mode {ONCE, LOOP}
    String name();
    Mode mode();
    void run() throws Exception;
}
