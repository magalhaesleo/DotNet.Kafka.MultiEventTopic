using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace UnionGenericRecord;

class Program
{
    public static async Task Main(string[] args)
    {
        const string topicName = "union-generic-record-topic-name-strategy";
        await Producer.Produce(
            new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 0 },
            new SchemaRegistryConfig { Url = "localhost:8081" },
            new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Topic,
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            },
            topicName: topicName
        );
        
        using var cts = new CancellationTokenSource();
        
        Console.CancelKeyPress += (_, e) =>
        {
            Console.WriteLine("\n[Ctrl+C Pressed] Requesting graceful shutdown...");
            
            // Prevent standard process termination
            e.Cancel = true; 
            
            // Trigger token cancellation
            // ReSharper disable once AccessToDisposedClosure
            cts.Cancel(); 
        };

        Consumer.Consume(
            new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            },
            new SchemaRegistryConfig
            {
                Url = "localhost:8081",
            },
            new AvroDeserializerConfig()
            {
                SubjectNameStrategy = SubjectNameStrategy.Topic
            },
            topicName,
            cts.Token
        );
    }
}