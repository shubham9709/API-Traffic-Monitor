package com.kafka.api_traffic_consumer.repository;

import com.kafka.api_traffic_consumer.entity.ServiceTrafficAnalytics;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ServiceTrafficAnalyticsRepository
        extends JpaRepository<ServiceTrafficAnalytics, Long> {
}