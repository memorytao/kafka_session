package com.kafka.project.app;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    public static String CURRENT_PATH = System.getProperty("user.dir");
    public static String RESOURCE_PATH = "/app/src/main/resources";
    public static String FILE_CONFIG = "/config.properties";

    public static void main(String[] args) {

        /*
         * There are 3 Topics ** updated on 15 Jan 2024
         * List of topics prepay-sync-master, prepay-sync-desc, prepay-sync-bundle-desc
         */

        // String topic = getPropertiesValue(args[0]);
        String topic = getPropertiesValue("prepay.sync.master.topic");
        createFileStatus(topic, "Failed|", "", getPropertiesValue("status.path"));
        consumeData(topic);
        File files = Paths.get(checkDirectory(getPropertiesValue("output.path"))).toFile();
        log.info(String.valueOf(files.list().length));

        if (files.list().length == 0) {
            createFileStatus(topic, "Failed|", "No change ", getPropertiesValue("status.path"));
        } else {
            createFileStatus(topic, "Success|", "", getPropertiesValue("status.path"));
            copyFileToBackup();
        }

    }

    public static void getLostConnection() {

        if (log.isErrorEnabled()) {
            log.error("-------");
        }

        
    }

    public static String getPropertiesValue(String configName) {

        try {

            FileInputStream fileInputStream = new FileInputStream(CURRENT_PATH + RESOURCE_PATH + FILE_CONFIG);
            Properties properties = new Properties();
            properties.load(fileInputStream);
            System.out.println(properties.getProperty(configName));
            return properties.getProperty(configName);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }

    public static void sendEmail() {

        try {

            Process process = Runtime.getRuntime().exec(getPropertiesValue("send.email.script"));

            try {
                int success = process.waitFor();

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } // Wait for script completion
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void createLatestOffset(long offset, long timestamp) {

        try {

            String offsetFile = "/latest_offset.txt";
            Path path = Paths.get(CURRENT_PATH + getPropertiesValue("status.path") + offsetFile);
            String content = String.valueOf(offset) + "," + String.valueOf(timestamp);
            Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage(), e.getCause());
        }
    }

    public static Map<String, Object> getLatestOffset() {

        try {

            String offsetFile = "/latest_offset.txt";
            Path path = Paths.get(CURRENT_PATH + getPropertiesValue("status.path") + offsetFile);
            Map<String, Object> resp = new HashMap<>();

            if (!path.toFile().exists()) {
                String initContent = "0,0";
                Files.writeString(path, initContent, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }

            List<String> offsets = Files.readAllLines(path);
            String[] latest = offsets.get(0).split(",");
            resp.put("offset", Long.valueOf(latest[0]));
            resp.put("timestamp", Long.valueOf(latest[1]));
            return resp;

        } catch (Exception e) {
            // TODO: handle exception
            log.error(" get offeset error " + e.getMessage(), e.getCause());
        }

        return null;
    }

    public static void copyFileToBackup() {

        checkDirectory(getPropertiesValue("backup.path"));
        Path source = Paths.get(CURRENT_PATH + getPropertiesValue("output.path"));
        Path target = Paths.get(CURRENT_PATH + getPropertiesValue("backup.path"));

        try {

            File files = source.toFile();

            for (String fileName : files.list()) {

                System.out.println(source.toAbsolutePath() + "/" + fileName);
                Files.copy(Paths.get(source.toAbsolutePath() + "/" + fileName),
                        target.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (Exception e) {
            // TODO: handle exception
            log.error(e.getMessage(), e.getCause());
        }

    }

    public static void createFileStatus(String topicName, String status, String details, String paths) {

        // main directory : /program_path/run/
        String path = checkDirectory(paths);
        String fileName = topicName + "_requested_status.txt";
        Path fileFullPath = Paths.get(path + "/" + fileName.replaceAll("-", "_"));
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
        String path = CURRENT_PATH + paths;
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

        String path = checkDirectory(getPropertiesValue("output.path"));
        String fileName = dateInString + "_" + topicName.replaceAll("-", "_") + "_ppm.txt";
        Path file = Paths.get(path + "/" + fileName);

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

            // long endTime = new Date().getTime();
            // endTime+=1200000;

            boolean isConsume = true;
            int countToShutDown = 0;

            consumer.subscribe(Arrays.asList(topic));

            Map<String, Object> latest = getLatestOffset();
            long latestOffset = (long) latest.get("offset");
            long latestTimeStamp = (long) latest.get("timestamp");

            while (isConsume) {
                // while (System.currentTimeMillis() < endTimeOfDay) {

                log.info("Cousuming..... ");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty())
                    countToShutDown++;
                if (countToShutDown == 20)
                    isConsume = false;

                

                for (ConsumerRecord<String, String> record : records) {

                    boolean isFistTimeConsume = record.offset() == 0 && latestTimeStamp < record.timestamp();
                    boolean isNewDataToConsume = latestOffset < record.offset();
                    if (isFistTimeConsume || isNewDataToConsume) {
                        latestOffset = -1;
                    }

                    if (record.offset() > latestOffset) {

                        log.info("offset : " + record.offset());
                        createPackageMasterFiles(topic, record.offset(), record.value());
                        createLatestOffset(record.offset(), record.timestamp());
                        countToShutDown = 0;
                    }
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
