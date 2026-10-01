package com.kafka.api_traffic_producer.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ApiTrafficEvent {

    private String requestId;
    private String serviceName;
    private String endpoint;
    private String httpMethod;
    private int statusCode;
    private long responseTimeMs;
    private long timestamp;

    public ApiTrafficEvent() {
    }

    public ApiTrafficEvent(
            String requestId,
            String serviceName,
            String endpoint,
            String httpMethod,
            int statusCode,
            long responseTimeMs,
            long timestamp) {

        this.requestId = requestId;
        this.serviceName = serviceName;
        this.endpoint = endpoint;
        this.httpMethod = httpMethod;
        this.statusCode = statusCode;
        this.responseTimeMs = responseTimeMs;
        this.timestamp = timestamp;
    }

    // getters and setters
}
