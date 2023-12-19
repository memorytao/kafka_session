package com.kafka.project.app;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class BIKafkaConsumer {

    public static Logger log = LoggerFactory.getLogger(BIKafkaConsumer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Start to consume...");

        KafkaConsumer<String, String> consumer = new KafkaConsumerConfigs().iniConsumer(null);

        final Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                // super.run();
                // log.info("Detected shutdown..............");
                consumer.wakeup();

                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

        try {

            long endTime = System.currentTimeMillis() + 20000;

            consumer.subscribe(Arrays.asList("wikimedia.recentchange"));
            for (; true ;) {

                log.info("Cousuming..... ");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    log.info("Key: " + record.key());
                    log.info("value: " + record.value());

                }
            }
        } catch (WakeupException wakeupException) {
            log.info("Consumer is starting shut down" + wakeupException.getMessage());

        } catch (Exception e) {
            // TODO: handle exception
            log.error(" Unexpected " + e.getMessage());
        } finally {

            consumer.close();
            log.info(" Consumer fully shutdown");
        }

    }
}
