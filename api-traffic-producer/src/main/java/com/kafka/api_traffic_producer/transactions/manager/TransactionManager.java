package com.kafka.api_traffic_producer.transactions.manager;

import com.kafka.api_traffic_producer.model.ApiTrafficEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

public class TransactionManager {
    @Bean
    public KafkaTransactionManager<String, ApiTrafficEvent> kafkaTransactionManager(
            ProducerFactory<String, ApiTrafficEvent> producerFactory) {

        return new KafkaTransactionManager<>(producerFactory);
    }
}
