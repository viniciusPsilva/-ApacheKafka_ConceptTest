package com.licao.kafka.kafkaLocalConnector.consumers;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
	
	 public CountDownLatch latch = new CountDownLatch(3);

	@KafkaListener(topics = "test", groupId="test-messages")
	public void listen(String message) {
		System.out.println("Mensagem recebida: "+ message);
		 latch.countDown();
	}
}
