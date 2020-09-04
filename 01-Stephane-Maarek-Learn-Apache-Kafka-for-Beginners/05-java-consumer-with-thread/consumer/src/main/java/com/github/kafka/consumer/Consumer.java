package com.github.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Consumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my_group";
        String topic = "my_topic";

        // Latch for dealing with multiple Threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Create Consumer Runnable
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, countDownLatch);

        // Start Consumer Thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");

            consumerRunnable.shutdown();

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            logger.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }
    }
}
