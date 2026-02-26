FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# install Debezium MySQL connector
RUN wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.5.4.Final/debezium-connector-mysql-2.5.4.Final-plugin.tar.gz \
    && tar -xzf debezium-connector-mysql-2.5.4.Final-plugin.tar.gz -C /usr/share/confluent-hub-components \
    && rm debezium-connector-mysql-2.5.4.Final-plugin.tar.gz

# install S3 sink connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0

USER appuser