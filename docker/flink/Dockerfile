FROM flink:1.18.1-scala_2.12-java17

RUN mkdir -p /opt/flink/usrlib /opt/flink/lib

# --- S3A support (avoid: No FileSystem for scheme: s3a) ---
# Flink S3 filesystem plugin (hadoop-based)
RUN curl -fSL -o /opt/flink/lib/flink-s3-fs-hadoop-1.18.1.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar

# Hadoop S3A implementation + AWS SDK bundle (match your hadoop.version=3.3.6)
RUN curl -fSL -o /opt/flink/lib/hadoop-aws-3.3.6.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

RUN curl -fSL -o /opt/flink/lib/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- Always include Kafka SQL connector (uber jar) in Flink lib ---
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.18.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

# --- Include Hudi Flink bundle ---
RUN curl -fSL -o /opt/flink/lib/hudi-flink1.18-bundle-1.1.1.jar \
  https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink1.18-bundle/1.1.1/hudi-flink1.18-bundle-1.1.1.jar

# fallback nếu bạn vẫn muốn copy jar local
COPY lib/*.jar /opt/flink/lib/
COPY usrlib/*.jar /opt/flink/usrlib/
COPY submit-hudi.sh /submit-hudi.sh
COPY conf/core-site.xml /opt/flink/conf/core-site.xml

ENV HADOOP_CONF_DIR=/opt/flink/conf

RUN chmod +x /submit-hudi.sh

