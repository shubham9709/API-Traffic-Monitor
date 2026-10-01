package com.kafka.api_traffic_analytics.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "service_traffic_analytics")
public class ServiceTrafficAnalytics {

    @Id
    private String serviceName;

    private long windowStart;

    private long windowEnd;

    private long requestCount;

    private long errorCount;

    private double averageLatencyMs;

}