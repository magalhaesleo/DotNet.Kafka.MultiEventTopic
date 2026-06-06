using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace AvroUnion;

public static class Producer
{
    public static async Task Produce(
        string topicName,
        string bootstrapServers,
        string schemaRegistryUrl)
    {
        ProducerConfig config = new() { BootstrapServers = bootstrapServers, LingerMs = 0 };
        SchemaRegistryConfig schemaRegistryConfig = new() { Url = schemaRegistryUrl };
        AvroSerializerConfig valueSerializerConfiguration = new()
        {
            SubjectNameStrategy = SubjectNameStrategy.Topic,
            AutoRegisterSchemas = false,
            UseLatestVersion = true
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, GenericRecord>(config)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, valueSerializerConfiguration))
            .Build();

        try
        {
            foreach (var message in Messages.Events())
            {
                var dr = await producer.ProduceAsync(topicName, message);
                Console.WriteLine($"Delivered '{dr.Value.Schema.Name}' to '{dr.TopicPartitionOffset}'");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}