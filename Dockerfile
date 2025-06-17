FROM apache/airflow:3.0.0

USER root

#RUN apt-get update && \
#    apt-get install -y gnupg2 curl unixodbc unixodbc-dev && \
#    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#    apt-get update && \
#    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
#    apt-get clean && \
#    rm -rf /var/lib/apt/lists/*

# Cài Java + các thư viện build cần thiết
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl gcc python3-dev libffi-dev libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME để PySpark có thể tìm thấy JVM
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

## Cài dependencies cần thiết để build một số thư viện Python như confluent-kafka
#RUN apt-get update && apt-get install -y gcc python3-dev libffi-dev libssl-dev

USER airflow

COPY ./proto /opt/airflow/proto
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/proto"

# Cài các thư viện Python
RUN pip install --no-cache-dir \
    "confluent-kafka[schema_registry]" \
    protobuf==4.25.3 \
    grpcio-tools==1.59.0 \
    fastavro \
    requests

RUN pip install --default-timeout=1200 --no-cache-dir pyspark