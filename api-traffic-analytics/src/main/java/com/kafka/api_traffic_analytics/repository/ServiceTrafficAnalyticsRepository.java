package com.kafka.api_traffic_analytics.repository;

import com.kafka.api_traffic_analytics.model.ServiceTrafficAnalytics;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ServiceTrafficAnalyticsRepository
        extends JpaRepository<ServiceTrafficAnalytics, Long> {

    List<ServiceTrafficAnalytics> findByServiceName(
            String serviceName
    );

    List<ServiceTrafficAnalytics> findByServiceNameOrderByWindowStartDesc(
            String serviceName
    );

    List<ServiceTrafficAnalytics>
    findByOrderByWindowStartDesc();
}