package com.kafka.api_traffic_consumer.consumer;

import com.kafka.api_traffic_consumer.entity.ServiceTrafficAnalytics;
import com.kafka.api_traffic_consumer.repository.ServiceTrafficAnalyticsRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsConsumer {

    private final ServiceTrafficAnalyticsRepository repository;

    public AnalyticsConsumer(ServiceTrafficAnalyticsRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
        topics = "service-traffic-analytics",
        groupId = "analytics-db",
            containerFactory = "serviceAnalyticsKafkaListenerContainerFactory"
    )
    public void consume(ServiceTrafficAnalytics analytics) {

        ServiceTrafficAnalytics entity =
                new ServiceTrafficAnalytics();

        entity.setServiceName(analytics.getServiceName());
        entity.setWindowStart(analytics.getWindowStart());
        entity.setWindowEnd(analytics.getWindowEnd());
        entity.setRequestCount(analytics.getRequestCount());
        entity.setErrorCount(analytics.getErrorCount());
        entity.setAverageLatencyMs(analytics.getAverageLatencyMs());

        repository.save(entity);
    }
}