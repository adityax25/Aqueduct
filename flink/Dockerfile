FROM flink:1.20.0

# Kafka SQL connector so Flink SQL can read and write Kafka topics,
# including the Debezium change-event format.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

# JDBC connector and PostgreSQL driver, so Flink SQL can write results to Postgres sink tables.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/flink/lib/

RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
               /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
               /opt/flink/lib/postgresql-42.7.4.jar
