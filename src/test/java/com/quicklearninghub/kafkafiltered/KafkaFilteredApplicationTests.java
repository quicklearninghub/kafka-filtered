package com.quicklearninghub.kafkafiltered;

import com.quicklearninghub.kafkafiltered.dto.MyDTO;
import com.quicklearninghub.kafkafiltered.enums.EventType;
import com.quicklearninghub.kafkafiltered.filter.RecordHeaderFilterStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = { "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}" })
@EmbeddedKafka(topics = "${kafka.topic}", partitions = 2)
@DirtiesContext
@ActiveProfiles("test")
public class KafkaFilteredApplicationTests {

	@Value(value = "${kafka.topic}")
	private String TOPIC;
	@Autowired
	private RecordHeaderFilterStrategy<String, MyDTO> recordHeaderFilterStrategy;

	@Autowired
	private KafkaTemplate<String, MyDTO> kafkaTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	private BlockingQueue<ConsumerRecord<String, MyDTO>> records;

	private KafkaMessageListenerContainer<String, MyDTO> container;

	@BeforeEach
	void setUp() {
		records = new LinkedBlockingDeque<>();

		Map<String, Object> consumerProps = new HashMap<>(
				KafkaTestUtils.consumerProps("${spring.kafka.consumer.group-id}", "true", embeddedKafkaBroker));
		DefaultKafkaConsumerFactory<String, MyDTO> defaultKafkaConsumerFactory =
				new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(),
								new JsonDeserializer<>(MyDTO.class, false));

		ContainerProperties containerProperties = new ContainerProperties(TOPIC);
		containerProperties.setMessageListener((MessageListener<String, MyDTO>) record -> {
			if(!recordHeaderFilterStrategy.filter(record)) {
				records.add(record);
			}
		});
		container = new KafkaMessageListenerContainer<>(defaultKafkaConsumerFactory, containerProperties);
		container.setCommonErrorHandler(new DefaultErrorHandler());
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
	}

	@AfterEach
	void tearDown() {
		container.stop();
	}

	@Test
	public void givenMessageWithHeader_whenConsumed_thenSuccess() throws InterruptedException, ExecutionException {
		// given
		MyDTO message = new MyDTO(EventType.NEWM, "Test new message");
		String key = "test-key";

		// when publishing a NEWM event
		kafkaTemplate.send(new ProducerRecord<>(TOPIC, null, key, message, getHeaders(message.getEventType()))).get();

		// then Record should be consumed
		ConsumerRecord<String, MyDTO> received = records.poll(10, TimeUnit.SECONDS);
		assertNotNull(received);
		assertEquals(key, received.key());
		assertEquals(message, received.value());
		assertEquals(message.getEventType().name(),
				new String(received.headers().lastHeader("eventType").value(), StandardCharsets.UTF_16), received.key());
		assertTrue(received.offset() >= 0);
	}

	@Test
	public void givenMessageWithoutHeader_whenNotConsumed_thenSuccess() throws InterruptedException, ExecutionException {
		// given
		MyDTO message = new MyDTO(EventType.CANC, "Test cancel message");
		String key = "test-key";

		// when publishing a CANC event
		kafkaTemplate.send(new ProducerRecord<>(TOPIC, null, key, message, getHeaders(message.getEventType()))).get();

		// then Record should not be consumed
		ConsumerRecord<String, MyDTO> received = records.poll(10, TimeUnit.SECONDS);
		assertNull(received);
	}

	private Headers getHeaders(EventType eventType) {
		Headers headers = new RecordHeaders();
		headers.add("eventType", eventType.name().getBytes(StandardCharsets.UTF_16));
		return headers;
	}
}

