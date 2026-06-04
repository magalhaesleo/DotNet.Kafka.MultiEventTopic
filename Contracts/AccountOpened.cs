using Avro;
using Avro.Specific;

namespace Contracts;

public class AccountOpened : ISpecificRecord
{
    public static readonly Schema _SCHEMA;

    static AccountOpened()
    {
        var json = File.ReadAllText("AccountOpened.avsc");
        _SCHEMA = Schema.Parse(json); 
    }
    
    public object Get(int fieldPos)
    {
        return fieldPos switch
        {
            0 => EventId,
            1 => AccountId,
            2 => OwnerName,
            3 => OpenedAt,
            _ => throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()")
        };
    }

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: EventId = (string)fieldValue; break;
            case 1: AccountId = (string)fieldValue; break;
            case 2: OwnerName = (string)fieldValue; break;
            case 3: OpenedAt = (DateTime)fieldValue; break;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }

    public Schema Schema => _SCHEMA;
    
    public string EventId { get; set; } = string.Empty;
    public string AccountId { get; set; } = string.Empty;
    public string OwnerName { get; set; } = string.Empty;
    public DateTime OpenedAt { get; set; }
}
