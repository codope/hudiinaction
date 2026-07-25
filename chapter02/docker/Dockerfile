FROM eclipse-temurin:11-jdk-jammy

ARG SPARK_VERSION=3.5.6
ARG HADOOP_VERSION=3
ARG HUDI_VERSION=1.2.0

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --ServerApp.token=''"

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip curl procps && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir jupyter

RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      | tar xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

RUN mkdir -p ${SPARK_HOME}/jars/hudi && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/${HUDI_VERSION}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar" \
      -o ${SPARK_HOME}/jars/hudi/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar

COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
RUN sed -i "s|__HUDI_VERSION__|${HUDI_VERSION}|g" ${SPARK_HOME}/conf/spark-defaults.conf

RUN mkdir -p /home/hudi/data /home/hudi/notebooks
COPY data/ /home/hudi/data/
COPY notebooks/ /home/hudi/notebooks/

WORKDIR /home/hudi/notebooks

EXPOSE 8888 4040

CMD ["pyspark"]
