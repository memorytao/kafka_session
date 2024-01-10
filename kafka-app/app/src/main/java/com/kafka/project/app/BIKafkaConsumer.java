package com.kafka.project.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
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

    public static String FILE_STATUS_PATH = "/app/src/main/resources/program_path/run";
    public static String FILE_OUTPUT_PATH = "/app/src/main/resources/program_path/output";
    public static String FILE_BACKUP_PATH = "/app/src/main/resources/program_path/backup";

    public static void main(String[] args) {

        // String topic = args[0];

        String topic = "demo_topic";

        createFileStatus(topic, "Failed|", "", FILE_STATUS_PATH);
        consumeData(topic);

        File files = Paths.get(checkDirectory(FILE_OUTPUT_PATH)).toFile();

        if (files.list().length == 0) {
            createFileStatus(topic, "Failed|", "Data not found", FILE_STATUS_PATH);
        } else {
            createFileStatus(topic, "Success|", "", FILE_OUTPUT_PATH);
            copyFileToBackup();
        }

    }

    public static void copyFileToBackup() {

        Path source = Paths.get(FILE_OUTPUT_PATH);
        Path target = Paths.get(FILE_BACKUP_PATH);

        try {

            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage(), e.getCause());
        }

    }

    public static void createFileStatus(String topicName, String status, String details, String paths) {

        // main directory : /program_path/run/
        String path = checkDirectory(paths);
        String fileName = topicName + "_requested_status.txt";
        Path fileFullPath = Paths.get(path + "/" + fileName);
        String content = status + details;
        try {

            Files.writeString(fileFullPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e.getMessage(), e.getCause());
        }

    }

    public static String checkDirectory(String paths) {

        // main directory : /program_path/run/
        String currentDir = System.getProperty("user.dir");
        String path = currentDir + paths;
        File file = new File(path);

        file.listFiles();

        if (!file.exists()) {
            file.mkdirs();
        }
        return file.getAbsolutePath();
    }

    public static void createPackageMasterFiles(String topicName, long offset, String value) {

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

        String path = checkDirectory(FILE_OUTPUT_PATH);
        String fileName = dateInString + "_" + topicName + "_ppm.txt";
        Path file = Paths.get(path + fileName);

        try {

            Files.writeString(file, data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e.getMessage(), e.getCause());
        }
    }

    public static void consumeData(String topicName) {

        log.info("Start to consume...");

        // String topic = "demo_topic";
        String topic = topicName;

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

            // int END_TIME_OF_DAY = 15;
            // Calendar endTime = Calendar.getInstance();
            // endTime.set(Calendar.HOUR_OF_DAY, END_TIME_OF_DAY);
            // long endTimeOfDay = endTime.getTime().getTime();

            boolean isConsume = true;
            int countToShutDown = 0;

            consumer.subscribe(Arrays.asList(topic));

            while (isConsume) {
                // while (System.currentTimeMillis() < endTimeOfDay) {

                log.info("Cousuming..... ");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.count() == 0)
                    countToShutDown++;
                if (countToShutDown == 20)
                    isConsume = false;

                for (ConsumerRecord<String, String> record : records) {

                    log.info("offset : " + record.offset() + " partition :" + record.partition() + " count :"
                            + records.count());
                    createPackageMasterFiles(topic, record.offset(), record.value());
                    countToShutDown = 0;
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
