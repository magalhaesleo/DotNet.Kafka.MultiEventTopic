using Avro;
using Avro.Generic;
using Confluent.Kafka;

namespace AvroUnion;

public static class Messages
{
    private static GenericRecord CreateAccountOpened(string accountId)
    {
        var schema = (RecordSchema)Schema.Parse(
            """
            {
              "type": "record",
              "name": "AccountOpened",
              "namespace": "DotNet.Kafka.MultiEventTopic",
              "fields": [
                {
                  "name": "eventId",
                  "type": "string"
                },
                {
                  "name": "accountId",
                  "type": "string"
                },
                {
                  "name": "ownerName",
                  "type": "string"
                },
                {
                  "name": "openedAt",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                }
              ]
            }
            """
        );

        GenericRecord record = new(schema);
        record.Add("accountId", accountId);
        record.Add("ownerName", "leonardo");
        record.Add("openedAt", DateTime.UtcNow);
        record.Add("eventId", Guid.NewGuid().ToString());
        return record;
    }

    private static GenericRecord CreateAccountBlocked(string accountId)
    {
        var schema = (RecordSchema)Schema.Parse(
            """
            {
              "type": "record",
              "name": "AccountBlocked",
              "namespace": "DotNet.Kafka.MultiEventTopic",
              "fields": [
                {
                  "name": "eventId",
                  "type": "string"
                },
                {
                  "name": "accountId",
                  "type": "string"
                },
                {
                  "name": "reason",
                  "type": "string"
                },
                {
                  "name": "occurredAt",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                }
              ]
            }
            """
        );
        
        GenericRecord record = new(schema);
        record.Add("accountId", accountId);
        record.Add("reason", "blocked by the bank");
        record.Add("occurredAt", DateTime.UtcNow);
        record.Add("eventId", Guid.NewGuid().ToString());
        return record;
    }
    
    private static GenericRecord CreateMoneyDeposited(string accountId)
    {
        var schema = (RecordSchema)Schema.Parse(
            """
            {
              "type": "record",
              "name": "MoneyDeposited",
              "namespace": "DotNet.Kafka.MultiEventTopic",
              "fields": [
                {
                  "name": "eventId",
                  "type": "string"
                },
                {
                  "name": "accountId",
                  "type": "string"
                },
                {
                  "name": "transactionId",
                  "type": "string"
                },
                {
                  "name": "amount",
                  "type": "double"
                },
                {
                  "name": "occurredAt",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                }
              ]
            }
            
            """
        );
        
        GenericRecord record = new(schema);
        record.Add("accountId", accountId);
        record.Add("amount", 100.0);
        record.Add("occurredAt", DateTime.UtcNow);
        record.Add("eventId", Guid.NewGuid().ToString());
        record.Add("transactionId", Guid.NewGuid().ToString());
        return record;
    }
    
    private static GenericRecord CreateMoneyWithdrawn(string accountId)
    {
        var schema = (RecordSchema)Schema.Parse(
            """
            {
              "type": "record",
              "name": "MoneyWithdrawn",
              "namespace": "DotNet.Kafka.MultiEventTopic",
              "fields": [
                {
                  "name": "eventId",
                  "type": "string"
                },
                {
                  "name": "accountId",
                  "type": "string"
                },
                {
                  "name": "transactionId",
                  "type": "string"
                },
                {
                  "name": "amount",
                  "type": "double"
                },
                {
                  "name": "occurredAt",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                }
              ]
            }
            
            """
        );
            
        GenericRecord record = new(schema);
        record.Add("accountId", accountId);
        record.Add("amount", 50.0);
        record.Add("occurredAt", DateTime.UtcNow);
        record.Add("eventId", Guid.NewGuid().ToString());
        record.Add("transactionId", Guid.NewGuid().ToString());
        return record;
    }
    
    public static IEnumerable<Message<string, GenericRecord>> Events()
    {
        var accountId = Guid.NewGuid().ToString();

        yield return new Message<string, GenericRecord>()
        {
            Key = accountId,
            Value = CreateAccountOpened(accountId)
        };

        yield return new Message<string, GenericRecord>()
        {
            Key = accountId,
            Value = CreateMoneyDeposited(accountId)
        };

        yield return new Message<string, GenericRecord>
        {
            Key = accountId,
            Value = CreateMoneyWithdrawn(accountId)
        };
        
        yield return new Message<string, GenericRecord>
        {
            Key = accountId,
            Value = CreateAccountBlocked(accountId)
        };
    }
}
