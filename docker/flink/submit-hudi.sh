#!/bin/sh
set -e

echo "Waiting Flink..."
sleep 12

echo "Submitting Hudi job..."
flink run -m jobmanager:8081 \
  -c com.example.hudi.Hudi \
  /opt/flink/usrlib/hudi-1.0.0-shaded.jar \
  --bootstrap=broker:9092 \
  --topic=output-topic \
  --groupId=hudi-writer \
  --basePath=s3a://hudi/student_perf \
  --hudiTable=student_perf \
  --minioEndpoint=http://minio:9000 \
  --minioAccessKey=minioadmin \
  --minioSecretKey=minioadmin \
  --minioBucket=hudi \
  --minioSsl=false

echo "Done."
