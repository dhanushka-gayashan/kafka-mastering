package com.kdg.kafka;

import com.kdg.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducer {

    private static Properties createConfig() {
        Properties config = new Properties();

        // Default Kafka Configurations
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "10");

        // AVRO Configurations - Serializer
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        return config;
    }

    private static Customer createCustomer() {
        return Customer.newBuilder()
                .setAge(34)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .setEmail("john.doe@gmail.com")
                .setPhoneNumber("(123)-456-7890")
                .build();
    }

    public static void main(String[] args) {
        Producer<String, Customer> producer = new KafkaProducer<>(createConfig());

        String topic = "customer-avro";
        Customer customer = createCustomer();
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}
