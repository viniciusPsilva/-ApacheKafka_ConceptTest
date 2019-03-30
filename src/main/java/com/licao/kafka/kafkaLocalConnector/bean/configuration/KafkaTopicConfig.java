package com.licao.kafka.kafkaLocalConnector.bean.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * @author viniciuspsilva
 * 
 * Classe responsável por criar um topico no kafka forma programatica
 *
 *
 */

@Configuration
public class KafkaTopicConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootStrapAddress;
	
	/**
	 * adiciona automaticamente tópicos para todos os beans do tipo NewTopic
	 * 
	 * @return KafkaAdimin
	 */
	@Bean
	public KafkaAdmin kafkaAdimin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapAddress);
		return new KafkaAdmin(configs);
	}
	
	/**
	 * Cria um novo topico 
	 * @return NewTopic
	 */
	@Bean
	public NewTopic topicTest() {
		return new NewTopic("test", 1, (short) 1 );
	}
}
