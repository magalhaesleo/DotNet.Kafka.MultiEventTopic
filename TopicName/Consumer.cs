using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace TopicName;

public static class Consumer
{
    public static void Consume(Settings settings, CancellationToken cancellationToken)
    {  
        var config = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var avroDeserializerConfig = new AvroDeserializerConfig()
        {
            SubjectNameStrategy = SubjectNameStrategy.Topic
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = settings.SchemaRegistryUrl,
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, BankAccountEvent>(config)
            .SetValueDeserializer(new AvroDeserializer<BankAccountEvent>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe(settings.TopicName);
        
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
                    case DepositOperation depositOperation:
                        Console.WriteLine($"Deposited {depositOperation.amount} to {bankAccountEvent.accountId}");
                        break;
                    case TransferOperation transferOperation:
                        Console.WriteLine(
                            $"Transferred {transferOperation.amount} from {bankAccountEvent.accountId} to {transferOperation.destinationAccountId}");
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