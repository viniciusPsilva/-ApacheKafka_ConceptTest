package com.licao.kafka.kafkaLocalConnector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import com.licao.kafka.kafkaLocalConnector.consumers.KafkaConsumer;
import com.licao.kafka.kafkaLocalConnector.consumers.ProdutoConsumer;
import com.licao.kafka.kafkaLocalConnector.models.Produto;
import com.licao.kafka.kafkaLocalConnector.producers.KafkaProducer;
import com.licao.kafka.kafkaLocalConnector.producers.ProdutoProducer;

@SpringBootApplication
public class KafkaLocalConnectorApplication {

	public static String hoje = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"));
	
	public static final String TOPICO_PRODUTOS = "produtos";
	 
	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaLocalConnectorApplication.class, args);

		//testEnviaMensagemTopicoTest(context);
		
		//testConsumirMensagemTopicoTest(context);
		
		//testEnviarMensagemTopicoProdutos(context);
		
		//testConsumirMensagemTopicoProdutos(context);
		
		testEnviaMensagemTopicoProdutosComHeaders(context);
	}

	
	/***
	 * Simula um consumidor para o topico de test
	 * @param context
	 * @throws InterruptedException
	 */
	private static void testConsumirMensagemTopicoTest(ConfigurableApplicationContext context) throws InterruptedException {
		KafkaConsumer kafkaConsumer = context.getBean(KafkaConsumer.class);
		kafkaConsumer.latch.await(10, TimeUnit.SECONDS);
	}

	/***
	 * Sumula um produtor de mensagens para o topico test
	 * @param context
	 */
	private static void testEnviaMensagemTopicoTest(ConfigurableApplicationContext context) {
		KafkaProducer producer = context.getBean(KafkaProducer.class);
		producer.sendMessage("test", "Teste mensagem dia " + hoje + " apartir do java...");
	}

	/**
	 * simula um consumidor de mensagens do tópico de produtos
	 * @param context
	 * @throws InterruptedException
	 */
	private static void testConsumirMensagemTopicoProdutos(ConfigurableApplicationContext context) throws InterruptedException {
		ProdutoConsumer produtoConsumer = context.getBean(ProdutoConsumer.class);
		produtoConsumer.latch.await(10, TimeUnit.SECONDS);
	}

	/***
	 * Simula um produtor de mensagens para o topico de produtos
	 * @param context
	 */
	private static void testEnviarMensagemTopicoProdutos(ConfigurableApplicationContext context) {
		ProdutoProducer produtoProducer = context.getBean(ProdutoProducer.class);
		produtoProducer.sendMessage("produtos", new Produto("Produto Test", "Produto teste realizado no dia " + hoje));
	}
	
	/***
	 * Simula um produtor de mensagens para o tópico de produtos, passando header customisado para o kafka cluster
	 * @param contex
	 */
	private static void testEnviaMensagemTopicoProdutosComHeaders(ConfigurableApplicationContext contex) {
		
		Produto produto = new Produto("Produto teste filter header 10:49", "produto teste header filter header");
		
		Message<Produto> message = MessageBuilder
				.withPayload(produto)
				.setHeader(KafkaHeaders.TOPIC, TOPICO_PRODUTOS)
				.setHeader(KafkaHeaders.MESSAGE_KEY, "key_test_header")
				.setHeader("eventType", new String("COMUNICACAO_CANAL_REALIZADA"))
				.build();
		
		ProdutoProducer produtoProducer = contex.getBean(ProdutoProducer.class);
		produtoProducer.sendMessageWhithHeaders(message);
		
	}

}
