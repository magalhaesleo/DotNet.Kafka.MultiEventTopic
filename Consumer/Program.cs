using Confluent.Kafka;

namespace Consumer;

public static class Program
{
    public static void Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
    }
}
