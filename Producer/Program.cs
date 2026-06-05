using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Kafka.MultiEventTopic;

namespace Producer;

public static class Program
{
    private static IEnumerable<ISpecificRecord> Events(string accountId)
    {
        yield return new AccountOpened
        {
            eventId = Guid.NewGuid().ToString(),
            accountId = accountId,
            ownerName = "",
            openedAt = DateTime.UtcNow
        };
        yield return new MoneyDeposited
        {
            eventId = Guid.NewGuid().ToString(),
            accountId = accountId,
            amount = 100,
            occurredAt = DateTime.UtcNow,
            transactionId = Guid.NewGuid().ToString()
        };
    }
    
    public static async Task Main(string[] args)
    {

    }
}
