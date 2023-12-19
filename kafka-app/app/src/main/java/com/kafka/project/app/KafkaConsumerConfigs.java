package com.kafka.project.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerConfigs {

  public KafkaConsumer<String, String> iniConsumer(Map<String, Object> props) {

    Map<String, Object> prop = new HashMap<>();

    String groupId = "first-group-id";
    
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    if (props != null && !props.isEmpty()) {
      prop.putAll(props);
    }

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

    return consumer;
  }
}
