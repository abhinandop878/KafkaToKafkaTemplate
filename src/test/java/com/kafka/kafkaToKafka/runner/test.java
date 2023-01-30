package com.kafka.kafkaToKafka.runner;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;
@RunWith(Cucumber.class)
@CucumberOptions(features ="src/test/resources/features/Kstream.feature",glue="com.kafka.kafkaToKafka.stepdef")
public class test {

}
