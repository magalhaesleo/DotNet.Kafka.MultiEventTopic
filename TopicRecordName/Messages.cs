using Avro.Specific;
using Confluent.Kafka;
using DotNet.Kafka.MultiEventTopic;

namespace TopicRecordName;

public static class Messages
{
    public static IEnumerable<Message<string, ISpecificRecord>> Events()
    {
        var accountId = Guid.NewGuid().ToString();

        yield return new Message<string, ISpecificRecord>
        {
            Key = accountId,
            Value = new AccountOpened
            {
                accountId = accountId,
                eventId = Guid.NewGuid().ToString(),
                ownerName = "leonardo",
                openedAt = DateTime.UtcNow,
            }
        };
        
        yield return new Message<string, ISpecificRecord>
        {
            Key = accountId,
            Value = new MoneyDeposited
            {
                accountId = accountId,
                amount = 100,
                eventId = Guid.NewGuid().ToString(),
                occurredAt = DateTime.UtcNow,
                transactionId = Guid.NewGuid().ToString(),
            }
        };
        
        yield return new Message<string, ISpecificRecord>
        {
            Key = accountId,
            Value = new MoneyWithdrawn()
            {
                accountId = accountId,
                amount = 50,
                eventId = Guid.NewGuid().ToString(),
                occurredAt = DateTime.UtcNow,
                transactionId = Guid.NewGuid().ToString(),
            }
        };
        
        yield return new Message<string, ISpecificRecord>
        {
            Key = accountId,
            Value = new AccountBlocked
            {
                accountId = accountId,
                eventId = Guid.NewGuid().ToString(),
                occurredAt = DateTime.UtcNow,
                reason = "blocked by the bank"
            }
        };
    }
}
