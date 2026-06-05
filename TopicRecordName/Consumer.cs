using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;
using YCherkes.SchemaRegistry.Serdes.Avro;

namespace TopicRecordName;

public static class Consumer
{
    public static void Consume(
        string topicName,
        string bootstrapServers,
        string schemaRegistryUrl,
        CancellationToken cancellationToken)
    {  
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var avroDeserializerConfig = new AvroDeserializerConfig()
        {
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl,
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, ISpecificRecord>(config)
            .SetValueDeserializer(new MultiSchemaAvroDeserializer(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
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
                switch (record)
                {
                    case AccountOpened accountOpened:
                        Console.WriteLine(
                            $"AccountOpened: {accountOpened.accountId} Owner: {accountOpened.ownerName} ");
                        break;
                    case MoneyDeposited moneyDeposited:
                        Console.WriteLine($"MoneyDeposited: {moneyDeposited.amount} to {moneyDeposited.accountId}");
                        break;
                    case MoneyWithdrawn moneyWithdrawn:
                        Console.WriteLine($"MoneyWithdrawn {moneyWithdrawn.amount} from: {moneyWithdrawn.accountId}");
                        break;
                    case AccountBlocked accountBlocked:
                        Console.WriteLine($"AccountBlocked {accountBlocked.accountId}");
                        break;
                    default:
                        Console.WriteLine("Unknown record: " + record?.GetType());
                        break;
                }
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}