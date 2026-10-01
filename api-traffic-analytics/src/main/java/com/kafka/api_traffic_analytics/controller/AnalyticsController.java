package com.kafka.api_traffic_analytics.controller;

import com.kafka.api_traffic_analytics.dto.AnalyticsResponse;
import com.kafka.api_traffic_analytics.model.ServiceTrafficAnalytics;
import com.kafka.api_traffic_analytics.service.AnalyticsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(
            AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @GetMapping
    public List<ServiceTrafficAnalytics> getAllAnalytics() {

        return analyticsService.getAllAnalytics();
    }

    @GetMapping("/services/{serviceName}")
    public List<AnalyticsResponse> getByService(
            @PathVariable String serviceName) {

        return analyticsService.getByService(serviceName);
    }
}