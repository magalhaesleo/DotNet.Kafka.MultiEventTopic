using Confluent.Kafka;
using DotNet.Kafka.MultiEventTopic;

namespace EmbeddedUnionType;

public static class Messages
{
    public static IEnumerable<Message<string, BankAccountEvent>> Events()
    {
        var accountId = Guid.NewGuid().ToString();

        yield return new Message<string, BankAccountEvent>()
        {
            Key = accountId,
            Value = new BankAccountEvent()
            {
                accountId = accountId,
                operation = new AccountOpened()
                {
                    accountId = accountId,
                    ownerName = "leonardo",
                    openedAt = DateTime.UtcNow,
                    eventId = Guid.NewGuid().ToString()
                }
            }
        };
        
        yield return new Message<string, BankAccountEvent>()
        {
            Key = accountId,
            Value = new BankAccountEvent()
            {
                accountId = accountId,
                operation = new MoneyDeposited
                {
                    accountId = accountId,
                    amount = 100,
                    eventId = Guid.NewGuid().ToString(),
                    occurredAt = DateTime.UtcNow,
                    transactionId = Guid.NewGuid().ToString()
                }
            }
        };

        yield return new Message<string, BankAccountEvent>
        {
            Key = accountId,
            Value = new BankAccountEvent
            {
                accountId = accountId,
                operation = new MoneyWithdrawn()
                {
                    accountId = accountId,
                    amount = 50,
                    eventId = Guid.NewGuid().ToString(),
                    occurredAt = DateTime.UtcNow,
                    transactionId = Guid.NewGuid().ToString(),
                }
            }
        };
        
        yield return new Message<string, BankAccountEvent>
        {
            Key = accountId,
            Value = new BankAccountEvent
            {
                accountId = accountId,
                operation = new AccountBlocked()
                {
                    accountId = accountId,
                    eventId = Guid.NewGuid().ToString(),
                    occurredAt = DateTime.UtcNow,
                    reason = "blocked by the bank"
                }
            }
        };
    }
}
