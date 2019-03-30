package com.licao.kafka.kafkaLocalConnector.bean.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.licao.kafka.kafkaLocalConnector.models.Produto;

/**
 * 
 * <p><b>Configura e cria os Bean necessários para produzir mensagen para o apache kafka</b></p> 
 * 
 * <p>Para criar mensagem primeiramente procisamos de um ProducerFactory,
 * que define a estratégia para criar as intâncias do kafka Producer.</p>
 * 
 * <p>Posteriormente precisaremos de um KafkaTemplate que envolva um instância do Producer
 * e forneça métodos convenientes para enviar mensagens para os tópicos do kafka</p>
 * 
 * @author viniciuspsilva
 *
 */

@Configuration
public class KafkaProducerConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootStrapAddress;
	
	@Bean
	public ProducerFactory<String, String> producerFactory(){
		Map<String, Object> configProps = new HashMap<String, Object>();
		
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapAddress);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		return new DefaultKafkaProducerFactory<>(configProps);
		
	}
	
	
	/**
	 * <p>Bean que envolve uma instância de um Producer, fornece métodos para enviar mensagens para os topicos Kafka</p>
	 * @return
	 */
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
	
	@Bean
	public ProducerFactory<String, Produto> producerFactoryProduto(){
		Map<String, Object> configProps = new HashMap<>();
		
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapAddress);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		
		return new DefaultKafkaProducerFactory<>(configProps);
	}
	
	@Bean
	public KafkaTemplate<String, Produto> kafkaTemplateProduto(){
		return new KafkaTemplate<>(producerFactoryProduto());
	}
	
}
