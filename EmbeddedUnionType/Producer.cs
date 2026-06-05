using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace EmbeddedUnionType;

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
        using var producer = new ProducerBuilder<string, BankAccountEvent>(config)
            .SetValueSerializer(new AvroSerializer<BankAccountEvent>(schemaRegistry, valueSerializerConfiguration))
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