package com.kafka.kafkaToKafka.stepdef;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.zookeeper.*;
import org.junit.Assert;
import java.util.concurrent.*;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MyStepdefs {
    private String messages;
    private Properties config;
    private TopologyTestDriver testDriver;
    @Given("Kafka and Zookeeper are installed and running")
    public void kafkaAndZookeeperAreInstalledAndRunning() {
        try {
            ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, null);
            if (zk.getState().isConnected()) {
                System.out.println("Zookeeper is running");
            }
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                System.out.println("Kafka is running");
            }
        } catch (Exception e) {
            System.out.println("Kafka and/or Zookeeper are not running: " + e.getMessage());
        }
    }

    @Then("They should be up and running")
    public void theyShouldBeUpAndRunning() {
        boolean isKafkaAndZookeeperRunning = false;
        try {
            ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, null);
            CountDownLatch connectedSignal = new CountDownLatch(1);
            zk.register(new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });
            connectedSignal.await();
            if (zk.getState().isConnected()) {
                System.out.println("Zookeeper is up and running");
            } else {
                throw new Exception("Zookeeper is not running");
            }
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                System.out.println("Kafka is up and running");
            }
            isKafkaAndZookeeperRunning = true;
        } catch (Exception e) {
            System.out.println("Kafka and/or Zookeeper are not running: " + e.getMessage());
        }

        Assert.assertTrue("Kafka and Zookeeper should be up and running", isKafkaAndZookeeperRunning);
    }

    @Given("a message {string} in input-topic")
    public void aMessageInInputTopic(String message) {
        messages=message;
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
    }

    @When("the message is processed by a Kafka Streams application")
    public void theMessageIsProcessedByAKafkaStreamsApplication() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
        testDriver = new TopologyTestDriver(builder.build(), config);
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("key", messages);
    }

    @Then("the message {string} should be in output-topic")
    public void theMessageShouldBeInOutputTopic(String msg) {
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer(), new StringDeserializer());
        KeyValue<String, String> output = outputTopic.readKeyValue();
        assertEquals("key", output.key);
        assertEquals(msg, output.value);
    }
}
