#!/usr/bin/env bash
set -euo pipefail

mkdir -p docker/hive/auxlib

HUDI_VER="1.1.1"

# Hadoop-aws/aws sdk: match versioin with Hadoop in image Hive.
# NoSuchMethodError: change version
HADOOP_AWS_VER="3.3.6"
AWS_SDK_BUNDLE_VER="1.12.262"

cd docker/hive/auxlib

echo "Downloading Hudi + S3A jars into $(pwd)"

curl -L -o hudi-hadoop-mr-bundle-${HUDI_VER}.jar \
  https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/${HUDI_VER}/hudi-hadoop-mr-bundle-${HUDI_VER}.jar

curl -L -o hadoop-aws-${HADOOP_AWS_VER}.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar

curl -L -o aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VER}.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VER}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VER}.jar

PG_JDBC_VER="42.7.3"
curl -L -o postgresql-${PG_JDBC_VER}.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VER}/postgresql-${PG_JDBC_VER}.jar

echo "Done yeyeye"
ls -lh