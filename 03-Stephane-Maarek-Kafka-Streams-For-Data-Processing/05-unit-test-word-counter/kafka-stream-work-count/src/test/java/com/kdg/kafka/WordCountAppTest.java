package com.kdg.kafka;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class WordCountAppTest {

    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        topologyTestDriver = new TopologyTestDriver(WordCountApp.createTopology(), config);

        inputTopic = topologyTestDriver.createInputTopic("word-count-input", new StringSerializer(), new StringSerializer());
        outputTopic = topologyTestDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void tearDown(){
        topologyTestDriver.close();
    }

    public void pushRecord(String record) {
        inputTopic.pipeInput(record);
    }

    public void pushRecords(List<String> record) {
        inputTopic.pipeValueList(record, Instant.ofEpochMilli(1L), Duration.ofMillis(100L));
    }

    public KeyValue<String, Long> readRecord() {
        return outputTopic.readKeyValue();
    }

    public Map<String, Long> readRecords() {
        return outputTopic.readKeyValuesToMap();
    }

    @Test
    public void testOneWord() {
        pushRecord("Hello");
        assertThat(readRecord(), equalTo(KeyValue.pair("hello", 1L)));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testMultipleWords() {
        pushRecord("testing Kafka Streams");
        assertThat(readRecord(), equalTo(KeyValue.pair("testing", 1L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("kafka", 1L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("streams", 1L)));
        assertTrue(outputTopic.isEmpty());

        pushRecord("testing Kafka again");
        assertThat(readRecord(), equalTo(KeyValue.pair("testing", 2L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("kafka", 2L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("again", 1L)));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testListsOfWords() {
        List<String> inputValues = Arrays.asList(
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit",
                "И теперь пошли русские слова"
        );

        Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("hello", 1L);
        expectedWordCounts.put("all", 1L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("lead", 1L);
        expectedWordCounts.put("to", 1L);
        expectedWordCounts.put("join", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("summit", 1L);
        expectedWordCounts.put("и", 1L);
        expectedWordCounts.put("теперь", 1L);
        expectedWordCounts.put("пошли", 1L);
        expectedWordCounts.put("русские", 1L);
        expectedWordCounts.put("слова", 1L);

        pushRecords(inputValues);
        assertThat(readRecords(), equalTo(expectedWordCounts));
    }

    @Test
    public void testAllWordsLowercase() {
        pushRecord("Kafka Kafka Kafka");
        assertThat(readRecord(), equalTo(KeyValue.pair("kafka", 1L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("kafka", 2L)));
        assertThat(readRecord(), equalTo(KeyValue.pair("kafka", 3L)));
        assertTrue(outputTopic.isEmpty());
    }
}
