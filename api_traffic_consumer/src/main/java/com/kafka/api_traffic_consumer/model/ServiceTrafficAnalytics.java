package com.kafka.api_traffic_consumer.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ServiceTrafficAnalytics {

    private String serviceName;

    private long windowStart;

    private long windowEnd;

    private long requestCount;

    private long errorCount;

    private double averageLatencyMs;

}