#!/bin/sh

readonly SCHEMA_URL="http://schema-registry:8081"

register_schema() {
  local file="$1"
  local subject="$2"
  
  jq -n --arg schema "$(cat $file)" '{schema: $schema}' | \
  curl -s -X POST -w "%{url}\n" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @- \
    "$SCHEMA_URL/subjects/$subject/versions"
}

# register TopicRecordName schemas
register_schema "/schemas/AccountBlocked.avsc" "multi-event-topic-record-name-strategy-DotNet.Kafka.MultiEventTopic.AccountBlocked"
register_schema "/schemas/AccountOpened.avsc" "multi-event-topic-record-name-strategy-DotNet.Kafka.MultiEventTopic.AccountOpened"
register_schema "/schemas/MoneyDeposited.avsc" "multi-event-topic-record-name-strategy-DotNet.Kafka.MultiEventTopic.MoneyDeposited"
register_schema "/schemas/MoneyWithdrawn.avsc" "multi-event-topic-record-name-strategy-DotNet.Kafka.MultiEventTopic.MoneyWithdrawn"

# register EnvelopePattern schema
register_schema "/schemas/BankAccountEvent.avsc" "multi-event-topic-envelope-pattern-value"

# root union schemas
register_schema "/schemas/AccountBlocked.avsc" "DotNet.Kafka.MultiEventTopic.AccountBlocked"
register_schema "/schemas/AccountOpened.avsc" "DotNet.Kafka.MultiEventTopic.AccountOpened"
register_schema "/schemas/MoneyDeposited.avsc" "DotNet.Kafka.MultiEventTopic.MoneyDeposited"
register_schema "/schemas/MoneyWithdrawn.avsc" "DotNet.Kafka.MultiEventTopic.MoneyWithdrawn"
jq -n \
  --rawfile schema /schemas/RootUnion.avsc \
  '{
    schema: $schema,
    references: [
      {
        "name":"DotNet.Kafka.MultiEventTopic.AccountBlocked",
        "subject":"DotNet.Kafka.MultiEventTopic.AccountBlocked",
        "version":1
      },
      {
        "name":"DotNet.Kafka.MultiEventTopic.AccountOpened",
        "subject":"DotNet.Kafka.MultiEventTopic.AccountOpened",
        "version":1
      },
      {
        "name":"DotNet.Kafka.MultiEventTopic.MoneyDeposited",
        "subject":"DotNet.Kafka.MultiEventTopic.MoneyDeposited",
        "version":1
      },
      {
        "name":"DotNet.Kafka.MultiEventTopic.MoneyWithdrawn",
        "subject":"DotNet.Kafka.MultiEventTopic.MoneyWithdrawn",
        "version":1
      }
    ]
  }' |
curl -s -X POST -w "%{url}\n" \
  $SCHEMA_URL/subjects/bank-events-root-union-schema-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @-
