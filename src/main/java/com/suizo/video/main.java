package com.suizo.video;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class main {

    public static void main(String[] args) throws IOException, InterruptedException {

        int producerThreads = 0,
                maxQueueLength = 0,
                consumerThreads = 0;

        List<String> directories = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("config.txt"))) {
            String line;
            boolean readingDirs = false;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (line.toLowerCase().startsWith("paste")) {
                    readingDirs = true;
                } else if (readingDirs) {
                    directories.add(line);
                } else {
                    String[] parts = line.split("=");

                    if (parts.length < 2 || parts[1].trim().isEmpty()) {
                        throw new IllegalArgumentException("Missing: " + parts[0]);
                    }

                    int val = Integer.parseInt(parts[1].trim());
                    if (parts[0].contains("producer-threads")) producerThreads = val;
                    else if (parts[0].contains("consumer-threads")) consumerThreads = val;
                    else if (parts[0].contains("max-queue-length")) maxQueueLength = val;
                }
            }

            if (producerThreads <= 0 ||
                    consumerThreads <= 0 ||
                    maxQueueLength <= 0 ||
                    directories.size() != producerThreads
            ) {
                throw new IllegalArgumentException("Config file is incomplete or directory count doesn't match producerThreads.\nAll 3 thread/queue variables and matching directories are required.");
            }

        } catch (FileNotFoundException e) {
            System.out.println("config.txt not found.");
            return;
        }

        // 1. Start gRPC Consumer Server & Web GUI Server
        ConsumerServer server = new ConsumerServer(50051, maxQueueLength, consumerThreads, 1024 * 1024 * 5);
        server.start();

        Thread serverThread = new Thread(() -> {
            try {
                server.blockUntilShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        serverThread.start();

        // 2. Run Multi-Threaded Producer Client
        ProducerClient client = new ProducerClient("localhost", 50051);
        ExecutorService producerPool = Executors.newFixedThreadPool(producerThreads);

        for (String dirPath : directories) {
            producerPool.submit(() -> client.processFolder(dirPath));
        }

        producerPool.shutdown();
        if (producerPool.awaitTermination(1, TimeUnit.HOURS)) {
            System.out.println("All directory processing completed.");
        } else {
            System.err.println("Upload processing timed out.");
        }

        client.shutdown();

        // 3. Keep the application alive so you can view the Web GUI at http://localhost:8080
        System.out.println("Uploads finished. Open http://localhost:8080 in your browser to preview videos. Press Ctrl+C to stop.");
        serverThread.join();
    }
}