package com.quicklearninghub.kafkafiltered.listener;


import com.quicklearninghub.kafkafiltered.dto.MyDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class KafkaFilteredListener {

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerFilteredFactory")
    public void listen(ConsumerRecord<String, MyDTO> consumerRecord) {
        log.info("Message consumed on topic: {}, offset {}, message {}", consumerRecord.topic(),
                consumerRecord.offset(), consumerRecord.value());
    }
}
