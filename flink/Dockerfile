FROM flink:1.20.0

# Kafka SQL connector so Flink SQL can read and write Kafka topics,
# including the Debezium change-event format.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar
