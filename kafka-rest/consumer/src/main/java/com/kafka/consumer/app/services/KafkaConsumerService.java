package com.kafka.consumer.app.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafka.consumer.app.config.KafkaConsumerConfigs;

@Service
public class KafkaConsumerService {

    @Autowired
    KafkaConsumerConfigs configs;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    @KafkaListener(groupId = "groupId", topics = "${kafka.topic}")
    public void getMessage(String smg) {

        System.out.println(smg);

    }

    public void addNewConfig() {
        configs.initKafkaConsumer(null);   
    }

}
