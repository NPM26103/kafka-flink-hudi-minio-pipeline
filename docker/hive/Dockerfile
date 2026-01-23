FROM apache/hive:3.1.3

USER root

# Add jars for S3A (MinIO) + Hudi read in Hive
RUN curl -fSL -o /opt/hive/lib/hadoop-aws-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -fSL -o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL -o /opt/hive/lib/hudi-hadoop-mr-bundle-1.1.1.jar \
    https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/1.1.1/hudi-hadoop-mr-bundle-1.1.1.jar

# (optional) commons-logging sometimes needed
RUN curl -fSL -o /opt/hive/lib/commons-logging-1.2.jar \
    https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar

COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hive/conf/core-site.xml

USER hive
