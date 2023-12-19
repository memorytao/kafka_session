package com.kafka.consumer.app.controller;

import org.springframework.web.bind.annotation.RestController;

import com.kafka.consumer.app.services.KafkaConsumerService;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
@RequestMapping("api/kafka")
public class KafkaController {

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    // public KafkaController(KafkaConsumerService kafkaConsumerService) {
    // this.kafkaConsumerService = kafkaConsumerService;
    // }

    @GetMapping("/additional")
    public String getMethodName() {
        kafkaConsumerService.addNewConfig();
        return "add";
    }

    @GetMapping("/consumer")
    public String getMessage() {
        // TODO: process POST request
        return "OK";
    }

}
