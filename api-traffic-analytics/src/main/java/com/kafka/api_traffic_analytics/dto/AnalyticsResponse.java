package com.kafka.api_traffic_analytics.dto;

import com.kafka.api_traffic_analytics.model.ServiceTrafficAnalytics;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AnalyticsResponse {

    private String serviceName;
    private long windowStart;
    private long windowEnd;
    private long requestCount;
    private long errorCount;
    private double averageLatencyMs;
    private double errorRate;

    public AnalyticsResponse() {
    }



    // getters/setters
}