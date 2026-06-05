using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace TopicNameUnion;

class Program
{
    private const string TopicName = "account-events-v4";
    
    private static async Task Produce()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081"
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        var accountId = Guid.NewGuid().ToString();
        var records = AccountData.Records();
        
        try
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 0 };
            var valueSerializerConfiguration = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Topic,
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
        
            using var producer = new ProducerBuilder<string, GenericRecord>(config)
                .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, valueSerializerConfiguration))
                .Build();

            foreach (var record in records)
            {
                var message = new Message<string, GenericRecord>()
                {
                    Key = accountId,
                    Value = record
                };
                var dr = await producer.ProduceAsync(TopicName, message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' schema {dr.Value.Schema.Fullname}");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
    
    public static async Task Main(string[] args)
    {
        await Produce();
        
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var avroDeserializerConfig = new AvroDeserializerConfig()
        {
            SubjectNameStrategy = SubjectNameStrategy.Topic
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe(TopicName);
        
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
                Console.WriteLine("RecordType: {0}", record.ToString());
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}