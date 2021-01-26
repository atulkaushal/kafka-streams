rem launch fast-data-dev docker image for testing this application.
docker run -it  -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 -e ADV_HOST=127.0.0.1 lensesio/fast-data-dev

rem access fast-data-dev UI (http://127.0.0.1:3030)

rem create input topic with two partition to get full ordering
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-color-input

rem create intermediary log compacted topic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-color-output --config cleanup.policy=compact

rem create output log compacted topic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic users-keys-and-colors --config cleanup.policy=compact

rem if there is a problem.
rem kafka-streams-application-reset.bat --application-id favourite-color-app1 --input-topics favourite-colour-input --intermediate-topics user-keys-and-colours

rem create output log compacted topic
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic favourite-color-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserialzer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

rem launch the streams application

rem then produce data to it
kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-color-input

rem
stephane,blue
john,green
stephane,red
alice,red

rem list all topics that we have in Kafka (so we can observe the internal topics)
kafka-topics.bat --list --zookeeper localhost:2181