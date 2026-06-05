using Avro;
using Avro.Generic;
using Avro.Specific;

namespace TopicNameUnion;

public static class SpecificRecordExtensions
{
    public static GenericRecord ToGenericRecord(this ISpecificRecord specificRecord)
    {
        var schema = (RecordSchema)specificRecord.Schema;
        var record = new GenericRecord(schema);
        
        for (var i = 0; i < schema.Count; i++)
            record.Add(i, specificRecord.Get(i));
        
        return record;
    }
}
