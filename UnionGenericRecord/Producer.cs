using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using TopicNameUnion;

namespace UnionGenericRecord;

public static class Producer
{
    public static async Task Produce(
        ProducerConfig config,
        SchemaRegistryConfig schemaRegistryConfig,
        AvroSerializerConfig serializerConfig,
        string topicName)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var accountId = Guid.NewGuid().ToString();
        var records = Messages.Records();
        
        try
        {
            using var producer = new ProducerBuilder<string, GenericRecord>(config)
                .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, serializerConfig))
                .Build();

            foreach (var record in records)
            {
                var message = new Message<string, GenericRecord>()
                {
                    Key = accountId,
                    Value = record
                };
                var dr = await producer.ProduceAsync(topicName, message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' schema {dr.Value.Schema.Fullname}");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}
