using Avro;
using Avro.Generic;

namespace TopicNameUnion;

public class AccountOpened
{
    private static readonly RecordSchema Schema = (RecordSchema)Avro.Schema.Parse(
        """
        {
          "type": "record",
          "name": "AccountOpened",
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

    public string EventId { get; set; }
    public string AccountId { get; set; }
    public string OwnerName { get; set; }
    public DateTime OpenedAt { get; set; }

    public GenericRecord ToGenericRecord()
    {
        var record = new GenericRecord(Schema);
        record.Add("eventId", EventId);
        record.Add("accountId", AccountId);
        record.Add("ownerName", OwnerName);
        record.Add("openedAt", OpenedAt);
        return record;
    }
}