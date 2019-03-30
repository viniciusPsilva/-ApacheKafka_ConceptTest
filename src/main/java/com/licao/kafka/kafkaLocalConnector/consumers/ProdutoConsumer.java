package com.licao.kafka.kafkaLocalConnector.consumers;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import com.licao.kafka.kafkaLocalConnector.models.Produto;

@Component
public class ProdutoConsumer {
	
	private static final Logger LOG = LoggerFactory.getLogger(ProdutoConsumer.class);
	
	public CountDownLatch latch = new CountDownLatch(1);
	
	/*@KafkaListener(topics = "${message.topic.produto.name}", containerFactory = "produtoKafkaListenerContainerFactory")
	public void listen(Produto produto) {
		LOG.info("Recebendo mesagem topic produtos :" + produto);
		System.out.println("Recebendo mensagem mesmo com dois consumers");
		this.latch.countDown();
	}*/
	
	@KafkaListener(topics = "${message.topic.produto.name}", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenWhithHeaders(Produto produto, @Headers MessageHeaders  messageHeaders) {
		LOG.info("Recebendo mesagem topic produtos com Headers :" + produto);
		
		LOG.info("-------------------------------------------------");
		LOG.info("VINI recebendo mensagens com headeers:'{}'", produto);
		Set<String> headerKeys = messageHeaders.keySet();		
		headerKeys.forEach(key -> {
			Object value = messageHeaders.get(key);
			if (key.equals("eventType")) {
				LOG.info("Meus Headers -> {}:{}", key, value);
			}
		});
		LOG.info("-------------------------------------------------");
		
		
		this.latch.countDown();
	}
}
