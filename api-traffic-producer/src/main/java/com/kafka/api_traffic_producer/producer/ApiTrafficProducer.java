package com.kafka.api_traffic_producer.producer;

import com.kafka.api_traffic_producer.model.ApiTrafficEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ApiTrafficProducer {
    private final KafkaTemplate<String, ApiTrafficEvent> kafkaTemplate;
    private static final String TOPIC= "api-traffic";

    public ApiTrafficProducer(KafkaTemplate<String, ApiTrafficEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(ApiTrafficEvent apiTrafficEvent){
        kafkaTemplate.send(TOPIC, apiTrafficEvent.getRequestId(), apiTrafficEvent);
    }
}
