using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace TopicName;

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
        using var consumer = new ConsumerBuilder<string, BankAccountEvent>(config)
            .SetValueDeserializer(new AvroDeserializer<BankAccountEvent>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe(topicName);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cancellationToken);
                if (consumeResult.IsPartitionEOF)
                    continue;
                
                var bankAccountEvent = consumeResult.Message.Value;
                switch (bankAccountEvent.operation)
                {
                    case AccountOpened accountOpened:
                        Console.WriteLine($"AccountOpened {accountOpened.accountId} Owner: {accountOpened.ownerName}");
                        break;
                    case MoneyDeposited moneyDeposited:
                        Console.WriteLine(
                            $"MoneyDeposited {moneyDeposited.amount} to: {moneyDeposited.accountId}");
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