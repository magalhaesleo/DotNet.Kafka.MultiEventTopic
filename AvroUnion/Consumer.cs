using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace AvroUnion;

public static class Consumer
{
    public static void Consume(
        string topicName,
        string bootstrapServers,
        string schemaRegistryUrl,
        CancellationToken cancellationToken)
    {
        ConsumerConfig config = new()
        {
            BootstrapServers = bootstrapServers,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        AvroDeserializerConfig avroDeserializerConfig = new() { SubjectNameStrategy = SubjectNameStrategy.Topic };
        SchemaRegistryConfig schemaRegistryConfig = new() { Url = schemaRegistryUrl };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe(topicName);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cancellationToken);
                if (consumeResult.IsPartitionEOF)
                    continue;
                
                var record = consumeResult.Message.Value;
                switch (record.Schema.Name)
                {
                    case "AccountOpened":
                        Console.WriteLine($"AccountOpened {record["accountId"]} Owner: {record["ownerName"]}");
                        break;
                    case "MoneyDeposited":
                        Console.WriteLine(
                            $"MoneyDeposited {record["amount"]} to: {record["accountId"]}");
                        break;
                    case "MoneyWithdrawn":
                        Console.WriteLine($"MoneyWithdrawn {record["amount"]} from: {record["accountId"]}");
                        break;
                    case "AccountBlocked":
                        Console.WriteLine($"AccountBlocked {record["accountId"]}");
                        break;
                    default:
                        Console.WriteLine("Unknown operation");
                        break;
                }
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}