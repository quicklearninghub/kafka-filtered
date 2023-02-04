package com.quicklearninghub.kafkafiltered.filter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.nonNull;

@Component
@Slf4j
public class RecordHeaderFilterStrategy<K,V> implements RecordFilterStrategy<K,V> {

    Charset charset = StandardCharsets.UTF_16;

    @Value("${kafka.eventType.filter.list:}")
    private List<String> eventTypes;

    /**
     * Return true if the record should be discarded.
     *
     * @param consumerRecord the record.
     * @return true to discard.
     */
    @Override
    public boolean filter(ConsumerRecord<K,V> consumerRecord) {
        if(nonNull(consumerRecord.headers())) {
            Iterator<Header> iterator = consumerRecord.headers().iterator();
            while(iterator.hasNext()) {
                Header header = iterator.next();
                String headerValue = new String(header.value(), charset);
                if(header.key().equalsIgnoreCase("eventType")
                        && eventTypes.contains(headerValue)) {
                    return false;
                }
            }
        }
        log.info("Message filtered(skipped) on topic: {}, offset {}", consumerRecord.topic(), consumerRecord.offset());
        return true;
    }

    /**
     * Filter an entire batch of records; to filter all records, return an empty list, not
     * null.
     *
     * @param list the records.
     * @return the filtered records.
     * @since 2.8
     */
    @Override
    public List<ConsumerRecord> filterBatch(List list) {
        return RecordFilterStrategy.super.filterBatch(list);
    }
}
