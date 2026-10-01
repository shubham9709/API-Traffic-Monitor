package com.kafka.api_traffic_consumer.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "api_traffic")
public class ApiTrafficEvent {
    @Id
    private String requestId;
    private String method;
    private String path;
    private int statusCode;
    private long latencyMs;
    private long timestamp;
    private String serviceName;


    public ApiTrafficEvent() {
    }

    public ApiTrafficEvent(
            String requestId,
            String method,
            String path,
            int statusCode,
            long latencyMs,
            long timestamp,
            String serviceName) {

        this.requestId = requestId;
        this.method = method;
        this.path = path;
        this.statusCode = statusCode;
        this.latencyMs = latencyMs;
        this.timestamp = timestamp;
        this.serviceName= serviceName;
    }

    // getters and setters
}
