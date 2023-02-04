package com.quicklearninghub.kafkafiltered.publisher;

import com.quicklearninghub.kafkafiltered.dto.MyDTO;
import com.quicklearninghub.kafkafiltered.enums.EventType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class KafkaPublisher {

    @Value(value = "${kafka.topic}")
    private String topic;

    private final KafkaTemplate<String, MyDTO> kafkaTemplate;

    public KafkaPublisher(KafkaTemplate<String, MyDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(MyDTO dto) {
        ProducerRecord<String, MyDTO> producerRecord = new ProducerRecord<>(topic, dto.getEventType().name(), dto);
        getHeaders(dto.getEventType()).forEach((k, v) -> {
                producerRecord.headers().add(k, v.getBytes(StandardCharsets.UTF_16));
        });
        kafkaTemplate.send(producerRecord);
        log.info("Message sent {}", dto);
    }

    private Map<String, String> getHeaders(EventType eventType) {
        Map<String, String> headers = new HashMap<>();
        headers.put("eventType", eventType.getEvent());
        return headers;
    }
}
