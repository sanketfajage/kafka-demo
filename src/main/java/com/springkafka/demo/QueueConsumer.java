package com.springkafka.demo;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class QueueConsumer {

	private static final AtomicBoolean closed = new AtomicBoolean(false);
	private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config());

	public static void main(String[] args) {
		run();
	}

	public static void run() {
		try {
			consumer.subscribe(Arrays.asList(Constants.TOPIC));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Key - " + record.key() + " Value - " + record.value());
					consumer.commitSync();
				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	private static Properties config() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constants.KAFKA_BROKERS);
		props.put("group.id", Constants.CONSUMER_GROUP);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		return props;
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
