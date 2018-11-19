package com.springkafka.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProducer {

	private static final Logger LOG = LoggerFactory.getLogger(QueueProducer.class);

	public static void main(String[] args) {
		sendMessage(Constants.TOPIC, "Sanket");
	}

	protected static Producer<String, String> producer = new KafkaProducer<>(initializeProducer());

	private QueueProducer() {
	}

	public static void sendMessage(String topic, String message) {
		if (producer == null) {
			initializeProducer();
		}

		LOG.info("Sending kafka message to topic {} using brokers {}", topic, Constants.KAFKA_BROKERS);

		ProducerRecord<String, String> data = new ProducerRecord<>(topic, message);
		producer.send(data);
		LOG.info("Sent");
		producer.flush();
	}

	protected static Properties initializeProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public void stop() {
		producer.close();
	}
}
