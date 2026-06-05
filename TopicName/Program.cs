namespace TopicName;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var settings = new Settings(
            BootstrapServers: "localhost:9092",
            SchemaRegistryUrl: "http://localhost:8081",
            TopicName: "account-events-topic-name-strategy"
        );

        Console.WriteLine("Sending messages...");
        await Producer.Produce(settings);
        
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
        Consumer.Consume(settings, cts.Token);
    }
}