package com.kdg.kafka;

import com.kdg.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {

    private static Properties createConfig() {
        Properties config = new Properties();

        // Default Kafka Configurations
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer-group-v1");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // AVRO Configurations - Deserializer
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        config.setProperty("specific.avro.reader", "true");

        return config;
    }

    public static void main(String[] args) {
        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(createConfig());

        String topic = "customer-avro";
        kafkaConsumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for data...");

        while (true){
            System.out.println("Polling...");
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, Customer> record : records){
                Customer customer = record.value();
                System.out.println(customer);
            }

            // Commit Offsets
            kafkaConsumer.commitSync();
        }
    }
}
