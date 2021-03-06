package com.github.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Producer.class);

        String bootstrapServers = "127.0.0.1:9092";

        // Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Produce and Send 10 messages : Same Key going to Same Partition
        for (int i = 0; i < 10; i++) {

            String topic = "my_topic";

            // Messages with same Key always going to same Partition
            String key = "id_" + i;

            String value = "hello world: " + i;

            // Create Produce Record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send Data with CallBack() - Asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("" +
                            "Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing ", e);
                }
            });
        }

        // Flush Data - Wait until data flush to broker
        producer.flush();

        // Close Producer
        producer.close();
    }
}
