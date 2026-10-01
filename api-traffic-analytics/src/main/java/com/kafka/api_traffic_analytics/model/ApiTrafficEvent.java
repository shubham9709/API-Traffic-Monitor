package com.kafka.api_traffic_analytics.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ApiTrafficEvent {
    private String requestId;
    private String method;
    private String path;
    private int statusCode;
    private long responseTimeMs;
    private long timestamp;
    private String serviceName;


    public ApiTrafficEvent() {
    }

    public ApiTrafficEvent(
            String requestId,
            String method,
            String path,
            int statusCode,
            long responseTimeMs,
            long timestamp,
            String serviceName) {

        this.requestId = requestId;
        this.method = method;
        this.path = path;
        this.statusCode = statusCode;
        this.responseTimeMs = responseTimeMs;
        this.timestamp = timestamp;
        this.serviceName= serviceName;
    }

    // getters and setters
}
