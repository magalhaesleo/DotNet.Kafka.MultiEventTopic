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

# register EmbeddedUnionType schema
register_schema "/schemas/BankAccountEvent.avsc" "multi-event-topic-embedded-union-type-value"
