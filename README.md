# Multiple Avro Message Types in a Kafka Topic with .NET

Examples demonstrating different approaches for producing and consuming multiple Avro message types in a single Kafka topic using .NET, Apache Kafka, and Confluent Schema Registry.

The examples use the following event types:

* `AccountOpened`
* `MoneyDeposited`
* `MoneyWithdrawn`
* `AccountBlocked`

Each event is defined as an independent Avro schema and published to the same Kafka topic.

## Partition Key

Messages are produced using `accountId` as the Kafka message key.

Using a consistent partition key ensures that all events for a given account are routed to the same partition, preserving message order and simplifying concurrent event processing.

## Projects

### EnvelopePattern

Uses an Avro union type to represent multiple event types within a single schema.

This approach is supported by the Confluent .NET serializer and deserializer.

### TopicRecordName

Uses the `TopicRecordNameStrategy` subject naming strategy, allowing multiple independent schemas to be associated with the same topic.

This example uses the `multi-schema-avro-deserializer` library to deserialize messages into their corresponding Avro types:

https://github.com/ycherkes/multi-schema-avro-deserializer

## Prerequisites

* Docker
* .NET SDK
* Docker Compose

## Running the infrastructure

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8080
```

## Running the examples

### EnvelopePattern

```bash
cd EnvelopePattern
dotnet run
```

### TopicRecordName

```bash
cd TopicRecordName
dotnet run
```

## References

* https://www.confluent.io/blog/put-several-event-types-kafka-topic/
* https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
* https://developer.confluent.io/courses/schema-registry/schema-subjects/
* https://github.com/ycherkes/multi-schema-avro-deserializer
