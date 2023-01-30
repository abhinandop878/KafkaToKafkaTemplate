Feature: Kafka Streams
  As an Architect, I should be able to use same template for different kafka-to-kafka streamlets.

  Scenario: Check Kafka and Zookeeper are running
    Given Kafka and Zookeeper are installed and running
    Then They should be up and running

  Scenario: Pass a message from input-topic to output-topic
    Given a message "hello" in input-topic
    When the message is processed by a Kafka Streams application
    Then the message "hello" should be in output-topic