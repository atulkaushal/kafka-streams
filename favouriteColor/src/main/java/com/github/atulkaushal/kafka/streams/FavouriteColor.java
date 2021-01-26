package com.github.atulkaushal.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/**
 * The Class FavouriteColor.
 *
 * @author Atul
 */
public class FavouriteColor {

  /** The Constant OUTPUT_TOPIC. */
  private static final String OUTPUT_TOPIC = "favourite-color-output";

  /** The Constant INPUT_TOPIC. */
  private static final String INPUT_TOPIC = "favourite-color-input";

  /** The Constant TEMP_TOPIC_FOR_PROCESSING. */
  private static final String TEMP_TOPIC_FOR_PROCESSING = "users-keys-and-colors";
  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> usersAndColors =
        builder
            .stream(INPUT_TOPIC)
            .filter((key, value) -> value.toString().contains(","))
            .selectKey((key, value) -> value.toString().split(",")[0].toLowerCase())
            .mapValues(value -> value.toString().split(",")[1].toLowerCase())
            .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

    usersAndColors.to(TEMP_TOPIC_FOR_PROCESSING, Produced.with(Serdes.String(), Serdes.String()));

    KTable<String, Long> favouriteColors =
        builder
            .table(TEMP_TOPIC_FOR_PROCESSING)
            .groupBy((user, color) -> new KeyValue(color, color))
            .count(Named.as("CountByColors"));

    // Write results back to Kafka
    favouriteColors.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.cleanUp(); // only for dev environment.
    streams.start();

    // shutdown hook to correctly close streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
