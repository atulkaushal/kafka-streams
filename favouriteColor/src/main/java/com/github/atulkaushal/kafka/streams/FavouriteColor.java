package com.github.atulkaushal.kafka.streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

/**
 * The Class FavouriteColor.
 *
 * @author Atul
 */
public class FavouriteColor {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
  }
}
