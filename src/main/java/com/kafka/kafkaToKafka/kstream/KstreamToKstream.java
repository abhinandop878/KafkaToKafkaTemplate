/*
 * Copyright (c) NeST Digital Pvt Ltd, 2023.
 * All Rights Reserved. Confidential.
 */
package com.kafka.kafkaToKafka.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;
/**

 * File: KstreamToKstream.java

 * This class is responsible for processing data streams using Kafka Streams API. It reads data from an input topic and
 * writes to an output topic. It uses Spring's @Value annotation to inject properties from application.properties file.

 @author Abhinand Manohar OP
 @date January 25, 2023

 */

@Component
public class KstreamToKstream {

    @Value("${kafka.streams.application.id}")
    private String applicationId;

    @Value("${kafka.streams.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.streams.default.key.serde}")
    private String keySerde;

    @Value("${kafka.streams.default.value.serde}")
    private String valueSerde;

    @Value("${kafka.streams.input.topic}")
    private String inputTopic;

    @Value("${kafka.streams.output.topic}")
    private String outputTopic;

    public String getApplicationId() {
        return applicationId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getKeySerde() {
        return keySerde;
    }

    public String getValueSerde() {
        return valueSerde;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    /**

     * Function: processing()

     * This method is responsible for configuring and starting the Kafka streams processing.
     * It sets up the properties for the streams, including the application ID, bootstrap servers, and default key and value serdes.
     * It then creates a StreamsBuilder and sets up the stream to read from the input topic and write to the output topic.
     * Finally, it creates a KafkaStreams object and starts the processing.

     */
    public void process() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic).to(outputTopic);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
   }

}
