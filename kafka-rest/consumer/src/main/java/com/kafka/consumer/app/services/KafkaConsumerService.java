package com.kafka.consumer.app.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafka.consumer.app.config.KafkaConsumerConfigs;

@Service
public class KafkaConsumerService {

    @Autowired
    KafkaConsumerConfigs configs;

    @KafkaListener(groupId = "GROUP_ID_1", topics = "demo_topic")
    public void getFirstTopic(ConsumerRecord<String, String> record) {
        System.out.println("First Topic value: " + record.value());

    }

    @KafkaListener(groupId = "GROUP_ID_2", topics = "demo_topic_2")
    public void getSecondTopic(ConsumerRecord<String, String> record) {

        System.out.println("Second Topic value: " + record.value());
    }

    public void addNewConfig() {
        configs.initKafkaConsumer(null);
    }

}
