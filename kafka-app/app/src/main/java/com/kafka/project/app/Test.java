package com.kafka.project.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Test {

    public static void main(String[] args) {

        // Calendar endTime = Calendar.getInstance();
        // endTime.set(Calendar.HOUR_OF_DAY, 18);

        // long endTimeOfDay = endTime.getTime().getTime();

        // System.out.println(System.currentTimeMillis() < endTimeOfDay);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String dateInString = simpleDateFormat.format(new Date());
        System.out.println(dateInString);

        String data = " {\n" + //
                "\t\"$schema\": \"/mediawiki/recentchange/1.0.0\",\n" + //
                "\t\"meta\": {\n" + //
                "\t\t\"uri\": \"https://en.wikipedia.org/wiki/Wikipedia:Articles_for_deletion/Log/2023_December_28\",\n"
                + //
                "\t\t\"request_id\": \"e20c6755-07e4-4112-9e0e-5de78ac7c91c\",\n" + //
                "\t\t\"id\": \"34111988-09b1-4c82-acbb-c0c1406c16b8\",\n" + //
                "\t\t\"dt\": \"2024-01-04T04:23:08Z\",\n" + //
                "\t\t\"domain\": \"en.wikipedia.org\",\n" + //
                "\t\t\"stream\": \"mediawiki.recentchange\",\n" + //
                "\t\t\"topic\": \"codfw.mediawiki.recentchange\",\n" + //
                "\t\t\"partition\": 0,\n" + //
                "\t\t\"offset\": 899873558\n" + //
                "\t},\n" + //
                "\t\"id\": 1711423712,\n" + //
                "\t\"type\": \"edit\",\n" + //
                "\t\"namespace\": 4,\n" + //
                "\t\"title\": \"Wikipedia:Articles for deletion/Log/2023 December 28\",\n" + //
                "\t\"title_url\": \"https://en.wikipedia.org/wiki/Wikipedia:Articles_for_deletion/Log/2023_December_28\",\n"
                + //
                "\t\"comment\": \"Relisting [[:Wikipedia:Articles for deletion/Service release premium]] ([[WP:XFDC#4.0.13|XFDcloser]])\",\n"
                + //
                "\t\"timestamp\": 1704342188,\n" + //
                "\t\"user\": \"Liz\",\n" + //
                "\t\"bot\": false,\n" + //
                "\t\"notify_url\": \"https://en.wikipedia.org/w/index.php?diff=1193504341&oldid=1193504278\",\n" + //
                "\t\"minor\": false,\n" + //
                "\t\"length\": {\n" + //
                "\t\t\"old\": 7444,\n" + //
                "\t\t\"new\": 7453\n" + //
                "\t},\n" + //
                "\t\"revision\": {\n" + //
                "\t\t\"old\": 1193504278,\n" + //
                "\t\t\"new\": 1193504341\n" + //
                "\t},\n" + //
                "\t\"server_url\": \"https://en.wikipedia.org\",\n" + //
                "\t\"server_name\": \"en.wikipedia.org\",\n" + //
                "\t\"server_script_path\": \"/w\",\n" + //
                "\t\"wiki\": \"enwiki\",\n" + //
                "\t\"parsedcomment\": \"Relisting <a href=\\\"/wiki/Wikipedia:Articles_for_deletion/Service_release_premium\\\" title=\\\"Wikipedia:Articles for deletion/Service release premium\\\">Wikipedia:Articles for deletion/Service release premium</a> (<a href=\\\"/wiki/Wikipedia:XFDC#4.0.13\\\" class=\\\"mw-redirect\\\" title=\\\"Wikipedia:XFDC\\\">XFDcloser</a>)\"\n"
                + //
                "}";

        JsonElement jsonElement = JsonParser.parseString(data);
        JsonObject jsonObject = jsonElement.getAsJsonObject();

        // System.out.println(jsonObject.keySet());

        String value = "";
        Object[] obj = jsonObject.keySet().toArray();
        for (int i = 0; i < jsonObject.keySet().size(); i++) {
            value += jsonObject.get(obj[i].toString()) + "|";
            if (obj.length - 1 == i)
                value += "\n";
        }

        System.out.println("VALUE : " + value);

        String path =
        "/Users/memorytao/development/kafka/kafka_session/kafka-app/app/src/main/resources/files/";
        String fileName = dateInString+".txt";

        Path file = Paths.get(path+fileName);

        try {
        Files.writeString(file, data);
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }

    }
}
