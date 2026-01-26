#!/bin/sh
set -e

echo "Waiting Flink..."
sleep 12

echo "Submitting Flink job..."
flink run -m jobmanager:8081 \
  -c com.example.flink.Flink \
  /opt/flink/usrlib/flink-1.0.0.jar \
  --bootstrap=broker:9092 \
  --groupId=flink-group \
  --ENABLE_CSV=true \
  --ENABLE_HTTP=true \
  --topic=raw.student.csv,raw.student.http \
  --outTopic=output-topic \
  --dlqTopic=output-topic-dlq \
  --checkpointMs=5000

echo "Done."