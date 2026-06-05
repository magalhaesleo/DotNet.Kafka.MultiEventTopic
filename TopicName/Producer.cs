using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace TopicName;

public static class Producer
{
    public static async Task Produce(Settings settings)
    {
        var config = new ProducerConfig { BootstrapServers = settings.BootstrapServers, LingerMs = 0 };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = settings.SchemaRegistryUrl
        };

        var valueSerializerConfiguration = new AvroSerializerConfig
        {
            SubjectNameStrategy = SubjectNameStrategy.Topic,
            AutoRegisterSchemas = true,
            UseLatestVersion = false
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, BankAccountEvent>(config)
            .SetValueSerializer(new AvroSerializer<BankAccountEvent>(schemaRegistry, valueSerializerConfiguration))
            .Build();
        
        try
        {
            foreach (var message in Messages.Events())
            {
                var dr = await producer.ProduceAsync(settings.TopicName, message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}