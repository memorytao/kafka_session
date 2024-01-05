package com.kafka.project.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BIKafkaConsumer {

    public static Logger log = LoggerFactory.getLogger(BIKafkaConsumer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Start to consume...");

        // String topic = args[0];
        String topic = "demo_topic";

        KafkaConsumer<String, String> consumer = new KafkaConsumerConfigs().iniConsumer(null);

        final Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                log.info("Detected shutdown..............");
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

            Calendar endTime = Calendar.getInstance();
            endTime.set(Calendar.HOUR_OF_DAY, 17);
            long endTimeOfDay = endTime.getTime().getTime();

            consumer.subscribe(Arrays.asList(topic));

            // consumer.currentLag(null);

            while (true) {
                // while (System.currentTimeMillis() < endTimeOfDay) {

                log.info("Cousuming..... ");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    log.info("offset : " + record.offset() + " partition :" + record.partition() + " count :"
                            + records.count());

                    createPackageMasterFile(record.offset(), record.value());

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

    public static void checkDirectory() {

        // String pathToFile = "/program_path/run/";
        String pathToFile = "../../../../../resources/files/";

        File directory = new File(pathToFile);
        if (!directory.exists()) {
            directory.mkdirs();
        }

    }

    public static void createPackageMasterFile(long offset, String value) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateInString = simpleDateFormat.format(new Date());

        JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();

        String data = "";
        Object[] obj = jsonObject.keySet().toArray();
        for (int i = 0; i < jsonObject.keySet().size(); i++) {
            data += offset + "|" + jsonObject.get(obj[i].toString()) + "|";
            if (obj.length - 1 == i)
                data += "\n";
        }

        String path = "/Users/memorytao/development/kafka/kafka_session/kafka-app/app/src/main/resources/files/";
        String fileName = dateInString + "_TOPIC_NAME_PPM.txt";
        Path file = Paths.get(path + fileName);

        try {

            Files.writeString(file, data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
