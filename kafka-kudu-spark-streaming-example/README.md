### Kafka, Spark Streaming and Kudu Integration Sample Code

#### To run the application:

1. Build from the project root directory:

    ```
    mvn clean package
    ```

2. Create the Kafka *traffic* topic (replication and partitions set to 1, for testing):

    ```
    kafka-topics --create --zookeeper <zookeeper server fqdn>:2181 --replication-factor 1 --topic traffic --partitions 1
    ```

3. On the client machine, create [client.properties](files/client.properties) file to connect with kafka producer.
   Here in this example we are using kerberos authentication.

4. Do kerberos login using `kinit`.

5. Produce simulated data on the topic (replace the kafka broker/port list parameter):

    ```
    while true; do echo "`date +%s%N | cut -b1-13`,$((RANDOM % 100))"; sleep 1; done | kafka-console-producer --broker-list <kafka-broker-fqdn>:9092,... --topic traffic --producer.config client.properties
    ```

6. Create [jaas.conf](files/jaas.conf) file on the client machine in another session.

7. Run the Spark Streaming application (replace kerberos user principal and keytab, kafka consumer group id, kafka brokers,
   and kudu masters parameters):

    ```
    SPARK_KAFKA_VERSION=0.10 spark2-submit
    --master yarn
    --deploy-mode cluster
    --class com.cazena.examples.KafkaKuduSparkStreamingJob
    --files <jaas.conf file>,<kerberos user keytab>
    --driver-java-options "-Djava.security.auth.login.config=jaas.conf"
    --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf"
    --jars /opt/cloudera/parcels/CDH/jars/kudu-spark2_2.11-1.7.0-cdh5.15.1.jar
    --principal <kerberos user principal>
    --keytab <kerberos user keytab>
    kafka-kudu-spark-streaming-example-1.0.jar <kafka consumer group id> <kafka broker fqdn>:9092 <kudu master fqdn>:7051
    ```

8. Create the Impala Kudu table using [Impala DDL script](files/create_impala_kudu_table.sql).

9. View the results in Kudu from Impala shell:

    ```
    select * from traffic_conditions order by as_of_time;
    ```

