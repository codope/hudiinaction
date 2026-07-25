FROM flink:1.20-scala_2.12

ENV HUDI_VERSION=1.2.0
ENV FLINK_VERSION=1.20
ENV CDC_VERSION=3.2.0

RUN wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink${FLINK_VERSION}-bundle/${HUDI_VERSION}/hudi-flink${FLINK_VERSION}-bundle-${HUDI_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/${CDC_VERSION}/flink-sql-connector-mysql-cdc-${CDC_VERSION}.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
