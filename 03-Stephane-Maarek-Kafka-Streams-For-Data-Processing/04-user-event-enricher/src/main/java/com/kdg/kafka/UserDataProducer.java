package com.kdg.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class UserDataProducer {

    private static Properties createConfig() {
        Properties config = new Properties();

        // Kafka Bootstrap Server
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer ACKS
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // Idempotent Producer - Ensure don't push duplicates
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return config;
    }

    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<>("user-purchases", key, value);
    }

    private static void printTitle(String title) {
        System.out.println(title);
    }

    private static void produceMassage(Producer<String, String> producer, ProducerRecord<String, String> record) throws Exception{
        producer.send(record).get();
    }

    private static void sleep() throws InterruptedException{
        Thread.sleep(10000);
    }

    public static void main(String[] args) throws Exception{
        Producer<String, String> producer = new KafkaProducer<String, String>(createConfig());

        // DO NOT USE ".get()" IN PRODUCTION OR IN ANY PRODUCER. BLOCKING A FUTURE IS BAD!
        // WE USE HERE TO DEMONSTRATION PURPOSE THAT ENSURE THE WRITES TO THE TOPICS ARE SEQUENTIAL

        // 1 - we create a new user, then we send some data to Kafka
        printTitle("\nExample 1 - new user\n");
        produceMassage(producer, userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com"));
        produceMassage(producer, purchaseRecord("john", "Apples and Bananas (1)"));
        sleep();

        // 2 - we receive user purchase, but it doesn't exist in Kafka
        printTitle("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();
        sleep();

        // 3 - we update user "john", and send a new transaction
        printTitle("\nExample 3 - update to user\n");
        produceMassage(producer, userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com"));
        producer.send(purchaseRecord("john", "Oranges (3)")).get();
        sleep();

        // 4 - we send a user purchase for stephane, but it exists in Kafka later
        printTitle("\nExample 4 - non existing user then user\n");
        produceMassage(producer, purchaseRecord("stephane", "Computer (4)"));
        produceMassage(producer, userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph"));
        produceMassage(producer, purchaseRecord("stephane", "Books (4)"));
        produceMassage(producer, userRecord("stephane", null)); /* delete for cleanup */
        sleep();

        // 5 - we create a user, but it gets deleted before any purchase comes through
        printTitle("\nExample 5 - user then delete then data\n");
        produceMassage(producer, userRecord("alice", "First=Alice"));
        produceMassage(producer, userRecord("alice", null)); /* that's the delete record */
        produceMassage(producer, purchaseRecord("alice", "Apache Kafka Series (5)"));
        sleep();

        printTitle("End of demo");
        producer.close();
    }
}
