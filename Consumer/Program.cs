using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;
using YCherkes.SchemaRegistry.Serdes.Avro;

namespace Consumer;

public static class Program
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var avroDeserializerConfig = new AvroDeserializerConfig()
        {
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        Type[] types = [typeof(AccountOpened), typeof(MoneyDeposited), typeof(AccountBlocked), typeof(AccountClosed)];
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, ISpecificRecord>(config)
            .SetValueDeserializer(new MultiSchemaAvroDeserializer(types, schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe("account-events");
        
        using var cts = new CancellationTokenSource();
        
        Console.CancelKeyPress += (_, e) =>
        {
            Console.WriteLine("\n[Ctrl+C Pressed] Requesting graceful shutdown...");
            
            // Prevent standard process termination
            e.Cancel = true; 
            
            // Trigger token cancellation
            cts.Cancel(); 
        };

        while (!cts.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cts.Token);
                if (consumeResult.IsPartitionEOF)
                    continue;
                
                var record = consumeResult.Message.Value;
                Console.WriteLine("Type: {0}", record.GetType());
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}
