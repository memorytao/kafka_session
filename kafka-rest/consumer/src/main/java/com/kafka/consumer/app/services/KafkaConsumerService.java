package com.kafka.consumer.app.services;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.micrometer.common.util.StringUtils;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "wikimedia.recentchange", id = "1")
    public void consume(String message) {

        if (StringUtils.isNotBlank(message)) {
            // System.out.println("Received message: " + message);

            JsonElement element = JsonParser.parseString(message);
            JsonObject jsonObject = element.getAsJsonObject();

            String name = jsonObject.get("user").getAsString();

            if (StringUtils.isNotBlank(name)) {

                System.out.println(name);
                // Path filePath = Paths.get(
                // "/Users/memorytao/development/kafka/kafka2/app/src/main/resources/files/" +
                // name + ".json");
                // try {
                // Files.write(filePath, message.getBytes(), StandardOpenOption.CREATE,
                // StandardOpenOption.WRITE);
                // } catch (IOException e) {
                // // TODO Auto-generated catch block
                // e.printStackTrace();
                // }
            }

        }

    }
}
