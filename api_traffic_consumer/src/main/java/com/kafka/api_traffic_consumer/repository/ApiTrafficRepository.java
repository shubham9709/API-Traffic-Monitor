package com.kafka.api_traffic_consumer.repository;

import com.kafka.api_traffic_consumer.model.ApiTrafficEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ApiTrafficRepository
        extends JpaRepository<ApiTrafficEvent, String> {
}