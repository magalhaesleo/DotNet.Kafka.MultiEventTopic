namespace EmbeddedUnionType;

public static class Program
{
    public static async Task Main(string[] args)
    {
        const string bootstrapServers = "localhost:9092";
        const string schemaRegistryUrl = "http://localhost:8081";
        const string topicName = "multi-event-topic-embedded-union-type";

        Console.WriteLine("Stating Kafka Producer and Consumer...");
        Console.WriteLine("Press Ctrl+C to stop the application.");
        Console.WriteLine();

        Console.WriteLine("Sending messages...");
        await Producer.Produce(
            topicName,
            bootstrapServers,
            schemaRegistryUrl
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

        Console.WriteLine("Consuming messages...");
        Consumer.Consume(topicName,
            bootstrapServers,
            schemaRegistryUrl,
            cts.Token
        );
    }
}