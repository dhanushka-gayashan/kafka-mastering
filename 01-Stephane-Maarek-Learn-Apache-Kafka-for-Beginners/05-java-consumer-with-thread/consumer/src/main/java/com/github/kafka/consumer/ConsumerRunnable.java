package com.github.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable{

    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    private final CountDownLatch countDownLatch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;

        // Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest / latest / none

        // Create Consumer
        consumer = new KafkaConsumer<>(properties);

        // Subscribe to Single Topic
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            // Poll for new Data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + " Value: " + record.value());
                    logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException e) {
            logger.info("Received shutdown signal!!");
        } finally {
            // Close Consumer Connections
            consumer.close();

            // Tell main code that we are done with the consumer
            countDownLatch.countDown();
        }
    }

    public void shutdown() {
        // The wakeup() method is a special method to interrupt consumer.poll()
        // It will throw the exception WakeUpException
        consumer.wakeup();
    }
}
