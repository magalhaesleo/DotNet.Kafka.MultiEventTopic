using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace TopicName;

public static class Program
{
    private const string TopicName = "account-events-topic";
    
    private static IEnumerable<Message<string, BankAccountEvent>> Events()
    {
        var accountId = Guid.NewGuid().ToString();

        yield return new Message<string, BankAccountEvent>()
        {
            Key = accountId,
            Value = new BankAccountEvent()
            {
                accountId = accountId,
                operation = new DepositOperation
                {
                    amount = 100,
                    source = "bank",
                }
            }
        };
        
        yield return new Message<string, BankAccountEvent>()
        {
            Key = accountId,
            Value = new BankAccountEvent()
            {
                accountId = accountId,
                operation = new TransferOperation()
                {
                    amount = 100,
                    destinationAccountId = Guid.NewGuid().ToString()
                }
            }
        };
    }
    private static async Task Produce()
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
        using var producer = new ProducerBuilder<string, BankAccountEvent>(config)
            .SetValueSerializer(new AvroSerializer<BankAccountEvent>(schemaRegistry, valueSerializerConfiguration))
            .Build();
        
        try
        {
            foreach (var message in Events())
            {
                var dr = await producer.ProduceAsync(TopicName, message);
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
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
        using var consumer = new ConsumerBuilder<string, BankAccountEvent>(config)
            .SetValueDeserializer(new AvroDeserializer<BankAccountEvent>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
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