package com.kafka.api_traffic_analytics.aggregate;

import com.kafka.api_traffic_analytics.model.ApiTrafficEvent;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ServiceAnalyticsAggregate {

    private long requestCount;
    private long errorCount;
    private long totalResponseTime;

    public ServiceAnalyticsAggregate() {
    }

    public void add(ApiTrafficEvent event) {
        requestCount++;
        totalResponseTime += event.getResponseTimeMs();

        if (event.getStatusCode() >= 500) {
            errorCount++;
        }
    }

    public double getAverageLatency() {
        if (requestCount == 0) {
            return 0;
        }

        return (double) totalResponseTime / requestCount;
    }
}