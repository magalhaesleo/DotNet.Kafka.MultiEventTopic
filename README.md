# Multiple Avro Message Types in a Kafka Topic with .NET

Examples demonstrating different approaches for producing and consuming multiple Avro message types in a single Kafka topic using .NET, Apache Kafka, and Confluent Schema Registry.

The examples use the following event types:

* `AccountOpened`
* `MoneyDeposited`
* `MoneyWithdrawn`
* `AccountBlocked`

## Partition key

Messages are produced using `accountId` as the Kafka message key.

Using a consistent partition key ensures that all events for a given account are routed to the same partition, preserving message order and simplifying concurrent event processing.

## Projects

### EnvelopePattern

Uses a wrapper schema named `BankAccountEvent` with a polymorphic `operation` field.

The `operation` field is an Avro union, so the serializer writes the concrete operation dynamically according to the event type while keeping one topic subject in Schema Registry (`multi-event-topic-envelope-pattern-value`).

### TopicRecordName

Uses the `TopicRecordNameStrategy` subject naming strategy, allowing multiple independent schemas to be associated with the same topic.

This example uses the `multi-schema-avro-deserializer` library to deserialize messages into their corresponding Avro types:

https://github.com/ycherkes/multi-schema-avro-deserializer

### AvroUnion

Uses a root-level Avro union schema registered for the topic value subject (`bank-events-root-union-schema-value`).

The union references the independent event schemas and the .NET producer/consumer use `GenericRecord` with the standard Confluent Avro serializer and deserializer. The consumer switches on the record schema name to handle each event type.

## Prerequisites

* Docker
* .NET SDK
* Docker Compose

## Running the infrastructure

```bash
docker compose up -d
```

The Compose setup starts Kafka, Schema Registry, Kafka UI, and a schema registration container that registers the schemas used by all examples.

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

### AvroUnion

```bash
cd AvroUnion
dotnet run
```

## References

* https://www.confluent.io/blog/put-several-event-types-kafka-topic/
* https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
* https://developer.confluent.io/courses/schema-registry/schema-subjects/
* https://github.com/ycherkes/multi-schema-avro-deserializer
