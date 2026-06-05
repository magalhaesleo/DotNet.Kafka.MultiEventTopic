namespace TopicRecordName;

public static class Program
{
    public static async Task Main(string[] args)
    {
        const string topicName = "multi-event-topic-record-name-strategy";
        const string bootstrapServers = "localhost:9092";
        const string schemaRegistryUrl = "http://localhost:8081";
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
        
        Consumer.Consume(topicName, bootstrapServers, schemaRegistryUrl, cts.Token);
    }
}
