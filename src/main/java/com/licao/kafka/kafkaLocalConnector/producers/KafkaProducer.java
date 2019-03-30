package com.licao.kafka.kafkaLocalConnector.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


/**
 * <p>Disponibiliza um método para enviar mensagens para um tópico no apache kafka</p>
 * 
 * @author viniciuspsilva
 *
 */
@Component
public class KafkaProducer {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	/**
	 * <p>Envia uma mensagem para um tópico no apache kafka</p>
	 * @param topicName
	 * @param message
	 */
	public void sendMessage(String topicName,String message) {
		kafkaTemplate.send(topicName, message);
	}
	
}
