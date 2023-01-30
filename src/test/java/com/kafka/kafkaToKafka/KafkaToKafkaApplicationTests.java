package com.kafka.kafkaToKafka;
import com.kafka.kafkaToKafka.kstream.KstreamToKstream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import java.util.Properties;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

@ExtendWith(MockitoExtension.class)
@SpringBootTest
class KafkaToKafkaApplicationTests {

    @Autowired
    private KstreamToKstream kstreamToKstream;

    @Mock
    private KstreamToKstream kstream;

    @InjectMocks
    private KafkaToKafkaApplication kafkaToKafkaApplication;

    @Test
    public void contextLoads() {
        kafkaToKafkaApplication.run();
        verify(kstream, times(1)).process();
    }

    @Test
    void testKstreamToKstream() {
        assertEquals("kafka-streams-demo", kstreamToKstream.getApplicationId());
        assertEquals("localhost:9092", kstreamToKstream.getBootstrapServers());
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde", kstreamToKstream.getKeySerde());
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde", kstreamToKstream.getValueSerde());
        assertEquals("input-topic", kstreamToKstream.getInputTopic());
        assertEquals("output-topic", kstreamToKstream.getOutputTopic());
    }
    private final String applicationId = "kafka-streams-demo";
    private final String bootstrapServers = "localhost:9092";
    private final String inputTopic = "input-topic";
    private final String outputTopic = "output-topic";
    @Test
    public void testProcessing() {
        kstreamToKstream.process();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic).to(outputTopic);
        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), config);
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(this.inputTopic, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("key", "value");
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(this.outputTopic, new StringDeserializer(), new StringDeserializer());
        KeyValue<String, String> output = outputTopic.readKeyValue();
        assertEquals("key", output.key);
        assertEquals("value", output.value);
    }
}