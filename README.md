# API Traffic Monitor

A distributed API traffic monitoring system built with **Java 17, Spring Boot, Apache Kafka, Kafka Streams, PostgreSQL, and Docker**.

The system captures API traffic events, publishes them to Kafka, processes the events using consumer groups and Kafka Streams, calculates service-level traffic analytics, and persists the results in PostgreSQL.


## Architecture

                         ┌──────────────────────┐
                         │   API Client / App   │
                         └──────────┬───────────┘
                                    │
                                    │ POST /api/traffic
                                    ▼
                         ┌──────────────────────┐
                         │   Producer Service   │
                         │   :8081              │
                         └──────────┬───────────┘
                                    │
                                    │ Kafka Producer
                                    ▼
                    ┌─────────────────────────────────┐
                    │           Kafka Cluster         │
                    │                                 │
                    │  KRaft Controller               │
                    │  Broker 1                       │
                    │  Broker 2                       │
                    │                                 │
                    │  Topic: api-traffic             │
                    └───────────────┬─────────────────┘
                                    │
                       ┌────────────┴────────────┐
                       │                         │
                       ▼                         ▼
              ┌─────────────────┐      ┌──────────────────┐
              │ Consumer Service │      │ Kafka Streams    │
              │     :8080       │      │ Analytics        │
              └────────┬────────┘      └────────┬─────────┘
                       │                         │
                       │                         │
                       ▼                         ▼
                ┌──────────────┐       ┌─────────────────────┐
                │ PostgreSQL   │       │ service-traffic-    │
                │              │       │ analytics topic     │
                └──────────────┘       └──────────┬──────────┘
                                                  │
                                                  ▼
                                         ┌──────────────────┐
                                         │ Analytics DB     │
                                         │ Consumer         │
                                         └────────┬─────────┘
                                                  │
                                                  ▼
                                           ┌──────────────┐
                                           │ PostgreSQL   │
                                           └──────────────┘

                                         Analytics Service
                                               :8083
                                                  │
                                    ┌─────────────┴─────────────┐
                                    │                           │
                              GET /analytics        GET /analytics/services/{serviceName}
```


## Features

* API traffic event ingestion through a REST API
* Kafka-based asynchronous event processing
* Kafka cluster with:

  * 1 KRaft controller
  * 2 Kafka brokers
* Replicated Kafka topics
* Consumer groups for independent processing
* JSON serialization/deserialization
* Kafka Streams for real-time aggregation
* Service-level request and error counting
* Average latency calculation
* Time-window based analytics
* PostgreSQL persistence
* Spring Data JPA
* Dockerized microservices
* Docker Compose orchestration
* Persistent Docker volumes for Kafka and PostgreSQL data


## Technology Stack

| Technology         | Purpose                           |
| ------------------ | --------------------------------- |
| Java 17            | Application development           |
| Spring Boot        | Microservices                     |
| Spring Kafka       | Kafka integration                 |
| Apache Kafka 4.3.1 | Event streaming                   |
| Kafka Streams      | Stream processing and aggregation |
| PostgreSQL 17      | Persistent storage                |
| Spring Data JPA    | Database persistence              |
| Maven              | Build and dependency management   |
| Docker             | Containerization                  |
| Docker Compose     | Multi-container orchestration     |


# Project Structure

api-traffic-monitor/
│
├── api-traffic-producer/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│
├── api_traffic_consumer/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│
├── api-traffic-analytics/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│
├── docker/
│   └── docker-compose.yaml
│
└── .gitignore

### Services

| Service          |   Port | Responsibility                                         |
| ---------------- | -----: | ------------------------------------------------------ |
| Producer         | `8081` | Accepts API traffic events and publishes them to Kafka |
| Consumer         | `8080` | Consumes traffic events and persists data              |
| Analytics        | `8083` | Exposes analytics APIs and processes analytics data    |
| PostgreSQL       | `5432` | Persistent database                                    |
| Kafka Broker 1   | `9092` | Kafka broker                                           |
| Kafka Broker 2   | `9192` | Kafka broker                                           |
| Kafka Controller | `9093` | KRaft controller                                       |


# How It Works

## 1. API traffic is submitted

A client sends an API traffic event to:

POST http://localhost:8081/api/traffic


Example request:

{
  "requestId": "req-1001",
  "method": "GET",
  "path": "/api/customers",
  "statusCode": 200,
  "latencyMs": 125,
  "timestamp": 1727700000000,
  "serviceName": "customer-service"
}

The producer service publishes the event to the Kafka topic:

api-traffic

The API responds with:

HTTP 202 Accepted


# Kafka Architecture

The application runs a Kafka cluster consisting of:

KRaft Controller
       │
       ├── Broker 1
       │
       └── Broker 2

Kafka runs in **KRaft mode**, without ZooKeeper.

The brokers communicate internally using Docker service names:

broker1:29092
broker2:29092

Host applications can connect through:

localhost:9092
localhost:9192

## Kafka Topics

### `api-traffic`

Main input topic containing API traffic events.

Producer
   │
   ▼
api-traffic
   │
   ├── Raw traffic consumer
   │
   └── Kafka Streams

### `service-traffic-analytics`

Output topic containing aggregated service-level analytics produced by Kafka Streams.

api-traffic
     │
     ▼
Kafka Streams
     │
     ▼
service-traffic-analytics

# Consumer Groups

The project demonstrates independent Kafka consumer groups.

## `traffic-monitor`

Used for processing the original API traffic events.

Kafka distributes records among consumers belonging to the same consumer group.

For example:

api-traffic
     │
     ├── Consumer 1
     ├── Consumer 2
     └── Consumer 3

Each record is processed by only one consumer within that group.


## `analytics-db`

Used for consuming the analytics output and persisting analytics data.

service-traffic-analytics
              │
              ▼
        analytics-db
              │
              ▼
          PostgreSQL

Because this is a separate consumer group, it can consume records independently of other consumer groups.


# Kafka Streams Processing

Kafka Streams consumes records from:

api-traffic

and performs service-level aggregation.

The stream processing groups traffic events by:

serviceName

and calculates metrics such as:

* Request count
* Error count
* Average latency

The aggregated data is written to:

service-traffic-analytics


## Analytics Model

Each analytics record contains:

private String serviceName;
private long windowStart;
private long windowEnd;
private long requestCount;
private long errorCount;
private double averageLatencyMs;


### `windowStart

The beginning of the analytics window represented as a Unix epoch timestamp in milliseconds.

### `windowEnd`

The end of the analytics window represented as a Unix epoch timestamp in milliseconds.

For example:

windowStart = 1727700000000
windowEnd   = 1727700060000

represents a 60-second window.


# PostgreSQL

The project uses PostgreSQL for persistent storage.

Database:

api_traffic

Default development credentials:


Username: postgres
Password: postgres


These credentials are intended for local Docker development.

The PostgreSQL data is stored in a Docker volume so that the database survives container restarts.


# REST APIs

## Producer API

### Publish API Traffic

POST /api/traffic

Full URL:

http://localhost:8081/api/traffic

### Request

{
  "requestId": "req-1001",
  "method": "GET",
  "path": "/api/customers",
  "statusCode": 200,
  "latencyMs": 125,
  "timestamp": 1727700000000,
  "serviceName": "customer-service"
}

### Response

HTTP 202 Accepted


# Analytics APIs

Analytics service runs on:

http://localhost:8083

## Get All Analytics


GET /analytics

Full URL:

http://localhost:8083/analytics

Example response:

[
  {
    "serviceName": "customer-service",
    "windowStart": 1727700000000,
    "windowEnd": 1727700060000,
    "requestCount": 120,
    "errorCount": 5,
    "averageLatencyMs": 142.75
  }
]

The timestamps are Unix epoch timestamps in milliseconds.


## Get Analytics for a Service

GET /analytics/services/{serviceName}

Example:

GET http://localhost:8083/analytics/services/customer-service

This endpoint returns analytics associated with the specified service.


# Running Locally

## Prerequisites

Install:

* Java 17
* Maven
* Docker
* Docker Compose

Verify:

java -version
mvn -version
docker --version
docker compose version

# Build the Applications

Build each Spring Boot application before starting Docker Compose.

### Producer

cd api-traffic-producer
./mvnw clean package -DskipTests

### Consumer

cd ../api_traffic_consumer
./mvnw clean package -DskipTests


### Analytics

cd ../api-traffic-analytics
./mvnw clean package -DskipTests

This creates the application JAR files under:

target/

# Start the Complete System

From the project root:

cd docker
docker compose up --build

This starts:

Kafka Controller
Kafka Broker 1
Kafka Broker 2
PostgreSQL
Producer
Consumer
Analytics

To run in detached mode:

docker compose up --build -d

# Check Running Containers

docker compose ps


Expected services:

kafka-controller
kafka-broker1
kafka-broker2
postgres-raw
traffic-producer
traffic-consumer
traffic-analytics

# View Logs

Producer:

docker logs -f traffic-producer

Consumer:

docker logs -f traffic-consumer

Analytics:

```bash
docker logs -f traffic-analytics
```

Kafka Broker 1:

docker logs -f kafka-broker1


Kafka Broker 2:

docker logs -f kafka-broker2

# Test the System

## 1. Send an API Traffic Event

Using `curl`:

curl -X POST http://localhost:8081/api/traffic \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "req-1001",
    "method": "GET",
    "path": "/api/customers",
    "statusCode": 200,
    "latencyMs": 125,
    "timestamp": 1727700000000,
    "serviceName": "customer-service"
  }'

Expected response:
HTTP 202 Accepted

## 2. Send Multiple Events

You can send multiple events with different:

* Services
* Status codes
* Latencies
* HTTP methods
* Paths

For example:
json
{
  "requestId": "req-1002",
  "method": "POST",
  "path": "/api/orders",
  "statusCode": 500,
  "latencyMs": 450,
  "timestamp": 1727700001000,
  "serviceName": "order-service"
}

## 3. Check Analytics

After the events are processed:

curl http://localhost:8083/analytics


Or:

curl http://localhost:8083/analytics/services/customer-service


# Docker Networking

The applications run inside the same Docker Compose network.

Therefore, containers communicate using Docker service names instead of `localhost`.

For example, the Spring Boot services connect to Kafka using:

broker1:29092
broker2:29092

and connect to PostgreSQL using:

postgres-raw:5432

### Important

Inside a Docker container:


localhost
```

means **the current container**, not another container.

Therefore:

jdbc:postgresql://localhost:5432/api_traffic

would attempt to connect to PostgreSQL inside the application container.

Instead, Docker services use:

jdbc:postgresql://postgres-raw:5432/api_traffic


# Docker Volumes

The project uses named Docker volumes to persist data.

volumes:
  controller-data:
  broker1-data:
  broker2-data:
  raw-postgres-data:

These volumes store:

* Kafka controller data
* Kafka Broker 1 data
* Kafka Broker 2 data
* PostgreSQL data

Therefore, restarting containers does not automatically remove the stored data.

To remove the containers **and their persistent volumes**:

docker compose down -v

Use this carefully because it removes the stored PostgreSQL and Kafka data.


# Configuration

The applications use Spring configuration properties for Kafka and PostgreSQL.

For Docker deployment, important properties are supplied through environment variables in `docker-compose.yaml`.

For example:

SPRING_KAFKA_BOOTSTRAP_SERVERS: broker1:29092,broker2:29092

and:

SPRING_DATASOURCE_URL: jdbc:postgresql://postgres-raw:5432/api_traffic

This allows the same applications to work with different infrastructure configurations without changing the application code.


# Kafka Connectivity

### From the host machine

Kafka brokers are exposed as:

localhost:9092
localhost:9192


### From Docker containers

Kafka brokers are accessed through:

broker1:29092
broker2:29092

This distinction exists because Docker containers and host applications use different network addresses.


# Data Flow

The complete processing flow is:

1. Client
      │
      │ POST /api/traffic
      ▼
2. Producer Service
      │
      │ Kafka Producer
      ▼
3. api-traffic topic
      │
      ├─────────────────────────────┐
      │                             │
      ▼                             ▼
4. Raw Consumer              5. Kafka Streams
      │                             │
      ▼                             ▼
 PostgreSQL             service-traffic-analytics
                                    │
                                    ▼
                           Analytics DB Consumer
                                    │
                                    ▼
                                PostgreSQL


The analytics service exposes the persisted analytics through REST APIs.


# Key Kafka Concepts Demonstrated

This project provides hands-on implementation of:

* Kafka producers
* Kafka consumers
* Consumer groups
* Topic partitions
* Kafka brokers
* Kafka replication
* KRaft mode
* Bootstrap servers
* Internal and external Kafka listeners
* JSON serialization/deserialization
* Kafka Streams
* Stream aggregation
* Window-based analytics
* Kafka output topics
* Persistent Kafka storage
* Dockerized Kafka infrastructure


# Why Kafka Is Used

API traffic can be generated at a high rate.

Instead of making the API request wait for every downstream operation, the producer publishes an event to Kafka and returns:

202 Accepted


The downstream processing happens asynchronously.

Kafka therefore acts as the event-streaming backbone between the API ingestion layer and processing components.


# Scalability

The architecture allows individual components to scale independently.

For example:

                    api-traffic
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Consumer 1  Consumer 2  Consumer 3


Consumers belonging to the same consumer group can process different partitions concurrently.

The Kafka cluster also contains two brokers, allowing topic partitions to be replicated across brokers.


# Fault Tolerance

Kafka topic replication is configured with a replication factor of:

2

for the default replicated topics.

The Kafka brokers maintain replicated log data, while Docker volumes provide persistent storage for the Kafka data directories.

PostgreSQL also uses a persistent Docker volume:

raw-postgres-data


# Development Notes

The Docker Compose setup is designed primarily for local development and demonstration.

The project currently uses:


PLAINTEXT

Kafka listeners and local PostgreSQL credentials.

For a production deployment, additional infrastructure and security configuration would be required, such as:

* TLS
* SASL authentication
* Secret management
* Production database credentials
* Monitoring
* Alerting
* Resource limits
* Kubernetes deployment
* Kafka ACLs
* Production-grade PostgreSQL configuration


# Possible Future Improvements

Potential extensions include:

* Add Redis for caching
* Add Grafana/Prometheus monitoring
* Add authentication and authorization
* Add Kafka Schema Registry
* Introduce Avro/Protobuf serialization
* Add dead-letter topics
* Add retry handling
* Add Kubernetes deployment
* Add CI/CD pipeline
* Add more detailed analytics
* Add real-time dashboard
* Add API rate monitoring
* Add anomaly detection
* Add alerting for high latency/error rates


# Project Goals

This project was built to demonstrate practical experience with:


Spring Boot
     +
Apache Kafka
     +
Kafka Streams
     +
Consumer Groups
     +
PostgreSQL
     +
Docker

