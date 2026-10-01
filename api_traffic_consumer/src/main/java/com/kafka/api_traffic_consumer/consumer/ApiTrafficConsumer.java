package com.kafka.api_traffic_consumer.consumer;

import com.kafka.api_traffic_consumer.model.ApiTrafficEvent;
import com.kafka.api_traffic_consumer.repository.ApiTrafficRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ApiTrafficConsumer {

    private final ApiTrafficRepository repository;

    public KafkaTemplate kafkaTemplate;

    public ApiTrafficConsumer(ApiTrafficRepository repository, KafkaTemplate kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate= kafkaTemplate;
    }

    @KafkaListener(
            topics = "api-traffic",
            groupId = "traffic-monitor"
    )
    public void consume(ApiTrafficEvent apiTrafficEvent){

        if (repository.existsById(apiTrafficEvent.getRequestId())) {
            return;
        }
        System.out.println("Received event: " + apiTrafficEvent);
        ApiTrafficEvent traffic = new ApiTrafficEvent();
        traffic.setRequestId(apiTrafficEvent.getRequestId());
        traffic.setMethod(apiTrafficEvent.getMethod());
        traffic.setPath(apiTrafficEvent.getPath());
        traffic.setStatusCode(apiTrafficEvent.getStatusCode());
        traffic.setLatencyMs(apiTrafficEvent.getLatencyMs());
        traffic.setServiceName(apiTrafficEvent.getServiceName());
        traffic.setTimestamp(apiTrafficEvent.getTimestamp());
        repository.save(traffic);

    }
}
