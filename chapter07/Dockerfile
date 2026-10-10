FROM flink:1.20-scala_2.12

ENV HUDI_VERSION=1.2.0
ENV FLINK_VERSION=1.20
ENV CDC_VERSION=3.2.0
ENV KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ENV MYSQL_JDBC_VERSION=8.0.27
ENV HADOOP_VERSION=3.3.6

# Hudi Flink bundle, Flink CDC (Apache coordinates since 3.1), Kafka connector.
# Flink CDC cannot ship the MySQL JDBC driver (GPL), and the Hudi Flink bundle
# expects Hadoop classes on the classpath, so both are added explicitly.
RUN wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink${FLINK_VERSION}-bundle/${HUDI_VERSION}/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/${CDC_VERSION}/flink-sql-connector-mysql-cdc-${CDC_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_JDBC_VERSION}/mysql-connector-java-${MYSQL_JDBC_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar

# Hudi tables and checkpoints live on a shared volume mounted at /tmp/hudi. Create it
# owned by the flink user, so the empty volume inherits that ownership.
RUN mkdir -p /tmp/hudi && chown -R flink:flink /tmp/hudi
