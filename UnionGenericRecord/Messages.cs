using Avro.Generic;

namespace TopicNameUnion;

public static class Messages
{
    public static IEnumerable<GenericRecord> Records()
    {
        var accountId = Guid.NewGuid().ToString();
        yield return new AccountOpened
        {
            AccountId = accountId,
            EventId = Guid.NewGuid().ToString(),
            OwnerName = "leonardo",
            OpenedAt = DateTime.UtcNow,
        }.ToGenericRecord();
        yield return new MoneyDeposited
        {
            AccountId = accountId,
            Amount = 100,
            EventId = Guid.NewGuid().ToString(),
            OccurredAt = DateTime.UtcNow,
            TransactionId = Guid.NewGuid().ToString(),
        }.ToGenericRecord();
    }
}