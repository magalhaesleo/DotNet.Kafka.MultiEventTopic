using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Contracts;

namespace Producer;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 0 };
        
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081"
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, AccountOpened>(config)
            .SetValueSerializer(new AvroSerializer<AccountOpened>(schemaRegistry))
            .Build();

        try
        {
            var accountOpened = new AccountOpened();
            var message = new Message<Null, AccountOpened> { Value = accountOpened };
            var dr = await producer.ProduceAsync("account-events", message);
            Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
}
