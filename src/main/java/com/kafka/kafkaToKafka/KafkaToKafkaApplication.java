/*
 * Copyright (c) NeST Digital Pvt Ltd, 2023.
 * All Rights Reserved. Confidential.
 */
package com.kafka.kafkaToKafka;

import com.kafka.kafkaToKafka.kstream.KstreamToKstream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**

 * File: KafkaToKafkaApplication.java

 * This is the main class of the Kafka To Kafka application.
 * It is responsible for initializing the Spring Boot application.
 * and starting the kstream processing.

 * @author Abhinand Manohar OP
 * @date January 25, 2023

 */
@SpringBootApplication
public class KafkaToKafkaApplication {

	@Autowired
	private KstreamToKstream kstream;

	public static void main(String[] args) {
		ApplicationContext context = SpringApplication.run(KafkaToKafkaApplication.class, args);
		KafkaToKafkaApplication kafkaToKafkaApplication = context.getBean(KafkaToKafkaApplication.class);
		kafkaToKafkaApplication.run();
	}
	public void run() {
		kstream.process();
	}
}


