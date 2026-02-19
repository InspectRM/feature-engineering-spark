FROM apache/spark:4.1.0

ENV HOME=/tmp
RUN pip3 install --no-cache-dir "pandas>=2.2.0" "pyarrow>=10.0.0" "protobuf>=3.20.0"

# Add RocksDB state-store jar required for stateful processing
RUN mkdir -p /opt/spark/jars && \
		curl -L -o /opt/spark/jars/spark-state-store-rocksdb_2.13-4.1.0.jar \
			https://repo1.maven.org/maven2/org/apache/spark/spark-state-store-rocksdb_2.13/4.1.0/spark-state-store-rocksdb_2.13-4.1.0.jar || true

COPY main.py /app/
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.kafka:kafka-clients:3.6.0", "--driver-memory", "2g", "--executor-memory", "2g", "/app/main.py"]