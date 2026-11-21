package com.suizo.video;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;



public class ConfigReader {

    public int producerThreads = 1;
    public int consumerThreads = 4;
    public int maxQueueLength = 100;
    public List<String> directories = new ArrayList<>();



    public ConfigReader() {

        try (BufferedReader br = new BufferedReader(new FileReader("config.txt"))) {
            String line;
            boolean readingDirs = false;


            while ((line = br.readLine()) != null) {
                line = line.trim();


                if (line.isEmpty() || line.toLowerCase().startsWith("paste")) {
                    if (line.toLowerCase().startsWith("paste")) readingDirs = true;
                    continue;
                }


                if (readingDirs) {
                    directories.add(line);
                } else {
                    String[] parts = line.split("=");


                    if (parts.length >= 2) {
                        int val = Integer.parseInt(parts[1].trim());
                        if (parts[0].contains("producer-threads")) producerThreads = val;
                        else if (parts[0].contains("consumer-threads")) consumerThreads = val;
                        else if (parts[0].contains("max-queue-length")) maxQueueLength = val;
                    }


                }


            }


        } catch (Exception e) {
            System.out.println("config.txt not found. Using defaults.");
        }


    }


}