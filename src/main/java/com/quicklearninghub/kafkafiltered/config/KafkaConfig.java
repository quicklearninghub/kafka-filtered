package com.quicklearninghub.kafkafiltered.config;

import com.quicklearninghub.kafkafiltered.filter.RecordHeaderFilterStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class KafkaConfig {

    private KafkaProps kafkaProps;

    private RecordHeaderFilterStrategy recordHeaderFilterStrategy;

    KafkaConfig(KafkaProps kafkaProps, RecordHeaderFilterStrategy recordHeaderFilterStrategy) {
        this.kafkaProps = kafkaProps;
        this.recordHeaderFilterStrategy = recordHeaderFilterStrategy;
    }

    @Bean("kafkaListenerFilteredFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory
                = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory(kafkaProps.consumerProps()));
        concurrentKafkaListenerContainerFactory.setRecordFilterStrategy(recordHeaderFilterStrategy);
        return concurrentKafkaListenerContainerFactory;
    }

}
