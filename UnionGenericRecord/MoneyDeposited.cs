using Avro;
using Avro.Generic;

namespace TopicNameUnion;

public class MoneyDeposited
{
    private static readonly RecordSchema Schema = (RecordSchema)Avro.Schema.Parse(
        """
        {
          "type": "record",
          "name": "MoneyDeposited",
          "namespace": "com.github.magalhaesleo",
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
    
    public string EventId { get; set; }
    public string AccountId { get; set; }
    public string TransactionId { get; set; }
    public double Amount { get; set; }
    public DateTime OccurredAt { get; set; }

    public GenericRecord ToGenericRecord()
    {
      var record = new GenericRecord(Schema);
       record.Add("eventId", EventId);
       record.Add("accountId", AccountId);
       record.Add("transactionId", TransactionId);
       record.Add("amount", Amount);
       record.Add("occurredAt", OccurredAt);
       return record;
    }
}
