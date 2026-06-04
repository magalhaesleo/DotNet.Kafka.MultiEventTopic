using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace Producer;

public static class Program
{
    private static IEnumerable<ISpecificRecord> Events()
    {
        yield return new AccountOpened
        {
            eventId = Guid.NewGuid().ToString(),
            accountId = Guid.NewGuid().ToString(),
            ownerName = "",
            openedAt = DateTime.UtcNow
        };
        yield return new MoneyDeposited
        {
            eventId = Guid.NewGuid().ToString(),
            accountId = Guid.NewGuid().ToString(),
            amount = 100,
            occurredAt = DateTime.UtcNow,
            transactionId = Guid.NewGuid().ToString()
        };
    }
    
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 0 };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081"
        };

        var valueSerializerConfiguration = new AvroSerializerConfig
        {
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord,
            AutoRegisterSchemas = true
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, ISpecificRecord>(config)
            .SetValueSerializer(new AvroSerializer<ISpecificRecord>(schemaRegistry, valueSerializerConfiguration))
            .Build();

        try
        {
            foreach (var @event in Events())
            {
                var message = new Message<Null, ISpecificRecord> { Value = @event };
                var dr = await producer.ProduceAsync("account-events", message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}
