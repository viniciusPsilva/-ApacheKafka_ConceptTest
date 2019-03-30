package com.licao.kafka.kafkaLocalConnector.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import com.licao.kafka.kafkaLocalConnector.models.Produto;

@Component
public class ProdutoProducer {

	@Autowired
	private KafkaTemplate<String, Produto> kafkaTemplateProduto;
	
	public void sendMessage(String topicName, Produto produto) {
		kafkaTemplateProduto.send(topicName, produto);
	}
	
	public void sendMessageWhithHeaders(Message<Produto> message) {
		kafkaTemplateProduto.send(message);
	}
	
}
