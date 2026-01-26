FROM apache/hive:4.0.0

USER root
RUN mkdir -p /opt/hive/auxlib

COPY docker/hive/auxlib/ /opt/hive/auxlib/

RUN cp -f /opt/hive/auxlib/postgresql-*.jar /opt/hive/lib/ || true

ENV HIVE_AUX_JARS_PATH=/opt/hive/auxlib
ENV HADOOP_CONF_DIR=/opt/hive/conf

USER hive
