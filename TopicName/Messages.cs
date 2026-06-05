using Confluent.Kafka;
using DotNet.Kafka.MultiEventTopic;

namespace TopicName;

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
}
