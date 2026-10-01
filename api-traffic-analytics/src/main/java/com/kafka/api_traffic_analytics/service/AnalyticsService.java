package com.kafka.api_traffic_analytics.service;

import com.kafka.api_traffic_analytics.dto.AnalyticsResponse;
import com.kafka.api_traffic_analytics.model.ServiceTrafficAnalytics;
import com.kafka.api_traffic_analytics.repository.ServiceTrafficAnalyticsRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AnalyticsService {

    private final ServiceTrafficAnalyticsRepository repository;

    public AnalyticsService(
            ServiceTrafficAnalyticsRepository repository) {
        this.repository = repository;
    }

    public List<ServiceTrafficAnalytics> getAllAnalytics() {
        return repository.findByOrderByWindowStartDesc();
    }

    public List<AnalyticsResponse> getByService(
            String serviceName) {

        return repository
                .findByServiceNameOrderByWindowStartDesc(serviceName)
                .stream()
                .map(this::toResponse)
                .toList();
    }
    private AnalyticsResponse toResponse(
            ServiceTrafficAnalytics entity) {

        AnalyticsResponse response = new AnalyticsResponse();

        response.setServiceName(entity.getServiceName());
        response.setWindowStart(entity.getWindowStart());
        response.setWindowEnd(entity.getWindowEnd());
        response.setRequestCount(entity.getRequestCount());
        response.setErrorCount(entity.getErrorCount());
        response.setAverageLatencyMs(
                entity.getAverageLatencyMs()
        );
        double errorRate = entity.getRequestCount() == 0
                ? 0
                : (entity.getErrorCount() * 100.0)
                / entity.getRequestCount();

        response.setErrorRate(errorRate);

        return response;
    }
}