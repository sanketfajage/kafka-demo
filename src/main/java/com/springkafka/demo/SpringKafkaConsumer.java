package com.springkafka.demo;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SpringKafkaConsumer {

	@KafkaListener(id = Constants.CONSUMER_GROUP, topics = Constants.TOPIC)
	public void listen(String in) {
		System.out.println("Received from topic: " + in);
	}
}
