# Kafka Producer Metrics Collector

This project provides a custom Kafka broker modification to collect and expose detailed producer metrics, including acknowledgment information, request bytes, and record counts per client, topic, partition, and IP address.

## Overview

The Producer Metrics Collector enhances Apache Kafka brokers by:
- Tracking producer acknowledgment metrics
- Monitoring request bytes and record counts
- Providing per-client, per-topic, per-partition metrics
- Including client IP address information
- Exposing metrics through Kafka's metrics system

## Features

- Metrics Collected:
  - producer_last_offset: Last acknowledged offset for each producer
  - producer_request_bytes_total: Total bytes sent by producer
  - producer_records_total: Total number of records sent
- Granularity: Metrics are tagged by clientId, topic, partition, and IP
- Cleanup: Automatic removal of stale metrics
- Thread-safe: Uses ConcurrentHashMap for metric storage
- Integration: Seamless integration with Kafka's metrics system

## Prerequisites

- Apache Kafka 3.x.x
- Scala 2.13.x
- Java 11 or higher
- Maven or SBT for building

## Installation

1. Apply Patch:
   Run the command: git apply KafkaApis_scala.patch
   This modifies KafkaApis.scala to intercept produce requests and responses.

2. Add Collector Code:
   Copy ProducerMetricsCollector.scala to core/src/main/scala/kafka/metrics/.

3. Build Kafka:
   Rebuild Kafka with the modified code: ./gradlew clean jar

4. Configure Broker:
   No additional configuration is required as the collector auto-initializes.

## Usage

1. Start Kafka broker with modified code.
2. Metrics will be automatically collected for all produce requests.
3. Access metrics through Kafka's metrics reporter (e.g., JMX, Prometheus).

### Available Metrics

| Metric Name                     | Description                                   | Labels                           |
|--------------------------------|-----------------------------------------------|----------------------------------|
| producer_last_offset           | Last acknowledged offset                     | clientId, topic, partition, ip   |
| producer_request_bytes_total   | Total bytes in produce requests              | clientId, topic, partition, ip   |
| producer_records_total         | Total number of records produced             | clientId, topic, partition, ip   |

### Cleanup

Stale metrics are automatically cleaned up. To customize cleanup, use:
ProducerMetricsCollector.getInstance.foreach(_.cleanupStaleMetrics(maxAgeMs))

## Monitoring

Configure your metrics reporter to scrape:
- Namespace: kafka.server
- Metrics group: ProducerMetricsCollector

For Prometheus, add to your configuration:
scrape_configs:
  - job_name: 'kafka-producer-metrics'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['broker:9999']

## Development

### Building

Run: mvn clean package

### Testing

Run Kafka's standard test suite after applying changes: ./gradlew unitTest

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with clear description

## License

This project is licensed under Apache License 2.0, same as Apache Kafka.

## Support

For issues, create a ticket in the GitHub issues tracker or contact the maintainers.