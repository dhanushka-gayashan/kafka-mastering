package com.kdg.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {

    private static Properties createConfig() {
        Properties config = new Properties();

        // Stream Configurations
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Consumer Configurations
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

    private static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Get a global table out of Kafka. This table will be replicated on each Kafka Streams application
        // The Key of GlobalKTable is the User ID
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

        // Get a stream of User Purchases
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        // Inner Join
        KStream<String, String> userPurchasesEnrichedInnerJoin =
                userPurchases.join(
                        usersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (userPurchase, userInfo) -> "UserInfo = [" + userInfo + "] : Purchase = [" + userPurchase + "]"
                );
        userPurchasesEnrichedInnerJoin.to("user-purchases-enriched-inner-join");

        // Left Join
        KStream<String, String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(
                        usersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (userPurchase, userInfo) -> {
                            /* as this is a left join, userInfo can be null */
                            if (userInfo != null) {
                                return "UserInfo = [" + userInfo + "] : Purchase = [" + userPurchase + "]";
                            } else {
                                return "UserInfo = [ Null ] : Purchase = [" + userPurchase + "]";
                            }
                        }
                );
        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        return builder.build();
    }

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(createTopology(), createConfig());

        streams.cleanUp();

        streams.start();

        streams.localThreadsMetadata().forEach(System.out::println);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
