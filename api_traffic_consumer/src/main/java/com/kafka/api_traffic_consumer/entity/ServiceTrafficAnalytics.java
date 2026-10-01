package com.kafka.api_traffic_consumer.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@Entity
@Table(name = "service_traffic_analytics")
public class ServiceTrafficAnalytics {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String serviceName;

    private long windowStart;

    private long windowEnd;

    private long requestCount;

    private long errorCount;

    private double averageLatencyMs;

    public ServiceTrafficAnalytics() {
    }

    // getters and setters
}