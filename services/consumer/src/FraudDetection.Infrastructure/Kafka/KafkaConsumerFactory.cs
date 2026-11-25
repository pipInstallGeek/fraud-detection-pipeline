using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Options;
using Confluent.Kafka;

namespace FraudDetection.Infrastructure.Kafka;

public class KafkaConsumerFactory
{
    private readonly ConsumerConfig _config;


    public KafkaConsumerFactory(IOptions<KafkaOptions> options)
    {
        var kafkaOptions = options.Value;
        
        _config = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.BootstrapServers,
            GroupId = kafkaOptions.GroupId,
            EnableAutoCommit = kafkaOptions.EnableAutoCommit,
            FetchMinBytes = kafkaOptions.FetchMinBytes,
            FetchWaitMaxMs = kafkaOptions.FetchWaitMaxMs,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(kafkaOptions.AutoOffsetReset, ignoreCase: true)
        };
    }

    public IConsumer<Ignore, byte[]> CreateRawConsumer()
    {
        return new ConsumerBuilder<Ignore, byte[]>(_config).Build();
    }
}