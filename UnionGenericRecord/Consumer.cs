using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace UnionGenericRecord;

public static class Consumer
{
    public static void Consume(
        ConsumerConfig config,
        SchemaRegistryConfig schemaRegistryConfig,
        AvroDeserializerConfig deserializerConfig,
        string topicName,
        CancellationToken cancellationToken)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry, deserializerConfig).AsSyncOverAsync())
            .Build();
        
        consumer.Subscribe(topicName);
        
        using var cts = new CancellationTokenSource();
        


        while (!cts.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cts.Token);
                if (consumeResult.IsPartitionEOF)
                    continue;
                
                var record = consumeResult.Message.Value;
                Console.WriteLine("RecordType: {0}", record.ToString());
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}