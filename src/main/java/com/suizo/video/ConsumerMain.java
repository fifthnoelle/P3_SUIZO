package com.suizo.video;

import java.io.IOException;



public class ConsumerMain {


    public static void main(String[] args) throws InterruptedException, IOException {


        // 1. read Config

        ConfigReader config = new ConfigReader();



        System.out.println("Starting Server...");
        System.out.println("Max Queue: " + config.maxQueueLength);
        System.out.println("Consumer Threads: " + config.consumerThreads + "\n\n");



        // 2. start Server

        ConsumerServer server = new ConsumerServer(

                50051,
                config.maxQueueLength,
                config.consumerThreads,
                1024 * 1024 * 5 // 5MB leaky bucket limit

        );



        server.start();
        server.blockUntilShutdown();



    }


}