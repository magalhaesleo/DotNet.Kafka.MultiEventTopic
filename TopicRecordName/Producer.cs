using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace TopicRecordName;

public static class Producer
{
    public static async Task Produce(
        string topicName,
        string bootstrapServers,
        string schemaRegistryUrl)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers, LingerMs = 0 };
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };

        var valueSerializerConfiguration = new AvroSerializerConfig
        {
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord,
            AutoRegisterSchemas = true,
            UseLatestVersion = false
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, ISpecificRecord>(config)
            .SetValueSerializer(new AvroSerializer<ISpecificRecord>(schemaRegistry, valueSerializerConfiguration))
            .Build();
        
        try
        {
            foreach (var message in Messages.Events())
            {
                var dr = await producer.ProduceAsync(topicName, message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}