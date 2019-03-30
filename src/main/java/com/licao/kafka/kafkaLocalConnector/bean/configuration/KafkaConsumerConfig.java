package com.licao.kafka.kafkaLocalConnector.bean.configuration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.licao.kafka.kafkaLocalConnector.models.Produto;

/**
 * <p>
 * <b>Configura um KafkaConsumer</b>
 * </p>
 * 
 * <p>
 * Para o consumo de mensagens precisamos configurar um ConsumerFactory e
 * KafkaListenerContainerFactory, <br>
 * Uma vez que esses bean estejam disponiveis na fabrica de bean do Spring, os
 * consumidores baseados em POJO <br>
 * podem ser configurados usando a anotação @KafkaListener
 * </p>
 * 
 * <p>
 * A anotação @EnableKafka é necessária na classe de configuração para permitir
 * a detecção da anotação
 * 
 * @kafkaListener em Bean gerenciados pelo Spring.
 *                </p>
 * 
 * @author viniciuspsilva
 *
 */

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	private static final String COMUNICACAO_CANAL_REALIZADA_MESSAGE_TYPE = "COMUNICACAO_CANAL_REALIZADA";

	public ConsumerFactory<String, String> consumerFactory(String groupId) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> fooKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory("test-messages"));
		return factory;
	}

	public ConsumerFactory<String, Produto> produtoConsumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "produtos");

		return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(),
				new JsonDeserializer<>(Produto.class));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Produto> produtoKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Produto> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(produtoConsumerFactory());
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Produto> filterKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Produto> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(produtoConsumerFactory());
		// regra para filtrar a mensagem pelo event type Header
		factory.setRecordFilterStrategy(record -> {
			Headers headers = record.headers();

			String headerValue = Stream.of(headers.toArray())
					.filter(h -> h.key()
					.equals("eventType"))
					.map(Header::value)
					.map(String::new)
					.findAny()
					.orElse("");

			if (headerValue.equals(COMUNICACAO_CANAL_REALIZADA_MESSAGE_TYPE)) {
				return false;
			}

			return true;

		});
		return factory;
	}

}
