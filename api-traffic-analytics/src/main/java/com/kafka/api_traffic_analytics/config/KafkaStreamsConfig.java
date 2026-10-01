package com.kafka.api_traffic_analytics.config;

import com.kafka.api_traffic_analytics.aggregate.ServiceAnalyticsAggregate;
import com.kafka.api_traffic_analytics.model.ApiTrafficEvent;
import com.kafka.api_traffic_analytics.model.ServiceTrafficAnalytics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, ApiTrafficEvent> trafficStream(
            StreamsBuilder streamsBuilder) {

        JacksonJsonSerde<ApiTrafficEvent> eventSerde =
                new JacksonJsonSerde<>(ApiTrafficEvent.class);

        eventSerde.ignoreTypeHeaders();
        JacksonJsonSerde<ServiceTrafficAnalytics> analyticsSerde =
                new JacksonJsonSerde<>(ServiceTrafficAnalytics.class);

        analyticsSerde.noTypeInfo();

        KStream<String, ApiTrafficEvent> stream =
                streamsBuilder.stream(
                        "api-traffic",
                        Consumed.with(
                                Serdes.String(),
                                eventSerde
                        )
                );

        KTable<Windowed<String>, ServiceAnalyticsAggregate> aggregated =
                stream
                        .groupBy(
                                (key, event) -> event.getServiceName(),
                                Grouped.with(
                                        Serdes.String(),
                                        eventSerde
                                )
                        )
                        .windowedBy(
                                TimeWindows.ofSizeWithNoGrace(
                                        Duration.ofMinutes(1)
                                )
                        )
                        .aggregate(
                                ServiceAnalyticsAggregate::new,
                                (serviceName, event, aggregate) -> {
                                    aggregate.add(event);
                                    return aggregate;
                                },
                                Materialized.<String, ServiceAnalyticsAggregate, WindowStore<Bytes, byte[]>>
                                                as("service-analytics-store")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(
                                                new JacksonJsonSerde<>(
                                                        ServiceAnalyticsAggregate.class
                                                )
                                        )
                        );

        aggregated
                .toStream()
                .mapValues((windowedKey, aggregate) -> {

                    ServiceTrafficAnalytics analytics =
                            new ServiceTrafficAnalytics();

                    analytics.setServiceName(windowedKey.key());

                    analytics.setWindowStart(
                            windowedKey.window().start()
                    );

                    analytics.setWindowEnd(
                            windowedKey.window().end()
                    );

                    analytics.setRequestCount(
                            aggregate.getRequestCount()
                    );

                    analytics.setErrorCount(
                            aggregate.getErrorCount()
                    );

                    analytics.setAverageLatencyMs(
                            aggregate.getAverageLatency()
                    );

                    return analytics;
                })
                .selectKey((windowedKey, analytics) ->
                        analytics.getServiceName())
                .to(
                        "service-traffic-analytics",
                        Produced.with(
                                Serdes.String(),
                                analyticsSerde
                        )
                );

        return stream;
    }
}