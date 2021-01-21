package com.github.atulkaushal.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class WordCountApp.
 *
 * @author Atul
 */
public class WordCountApp {

  /** The logger. */
  Logger logger = LoggerFactory.getLogger(WordCountApp.class.getName());

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    // 1. Stream from Kafka

    KStream<String, String> wordCountInput = builder.stream("word-count-input");

    // 2. map values to lower case
    KTable<String, Long> wordCounts =
        wordCountInput
            .mapValues(value -> value.toLowerCase())

            // 3. Flat map values split by space
            .flatMapValues(value -> Arrays.asList(value.split(" ")))

            // 4. Select Key to apply a key.
            .selectKey((ignoredKey, word) -> word)

            // 5. Group by key before aggregation
            .groupByKey()
            // 6. Count occurrences
            .count();
    // 7. Write results back to Kafka
    wordCounts.toStream().to("word-count-output");

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);

    streams.start();

    // shutdown hook to correctly close streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
