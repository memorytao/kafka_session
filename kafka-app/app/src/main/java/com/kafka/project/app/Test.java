package com.kafka.project.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Test {

    public static String CURRENT_PATH = System.getProperty("user.dir");
    public static String RESOURCE_PATH = "/app/src/main/resources";
    public static String FILE_CONFIG = "/config.properties";

    public static void main(String[] args) {

        try {

            checkDirectory(CURRENT_PATH + RESOURCE_PATH);
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

}
