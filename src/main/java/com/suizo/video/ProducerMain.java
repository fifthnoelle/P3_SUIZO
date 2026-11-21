package com.suizo.video;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



public class ProducerMain {


    public static void main(String[] args) throws InterruptedException {


        // 1. Read Config

        ConfigReader config = new ConfigReader();



        if (config.directories.isEmpty() || config.directories.size() != config.producerThreads) {
            System.err.println("Error: Directory count must match producer threads.");
            return;
        }


        System.out.println("Starting Producers: " + config.producerThreads + " threads.");



        // 2. Start Client Connections

        ProducerClient client = new ProducerClient("localhost", 50051);
        ExecutorService producerPool = Executors.newFixedThreadPool(config.producerThreads);



        // 3. Assign Folders to Threads

        for (String dirPath : config.directories) {
            producerPool.submit(() -> client.processFolder(dirPath));
        }

        producerPool.shutdown();


        if (producerPool.awaitTermination(1, TimeUnit.HOURS)) {
            System.out.println("All uploads finished.");

        } else {
            System.err.println("Uploads timed out.");
        }

        client.shutdown();


    }


}