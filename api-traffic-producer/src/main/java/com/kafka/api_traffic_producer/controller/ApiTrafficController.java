package com.kafka.api_traffic_producer.controller;

import com.kafka.api_traffic_producer.model.ApiTrafficEvent;
import com.kafka.api_traffic_producer.producer.ApiTrafficProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/traffic")
public class ApiTrafficController {

    private final ApiTrafficProducer producer;

    public ApiTrafficController(ApiTrafficProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Void> publish(@RequestBody ApiTrafficEvent event) {


        producer.send(event);

        return ResponseEntity.accepted().build();
    }
}
