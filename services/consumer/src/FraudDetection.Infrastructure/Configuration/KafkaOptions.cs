    namespace FraudDetection.Infrastructure.Configuration;


public class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";

    public string Topic { get; set; } = "cdc.public.transactions";

    public string TopicDlq { get; set; } = "cdc.public.transactions.dlq";

    public string GroupId { get; set; } = "fraud-consumer-group";

    public string AutoOffsetReset { get; set; } = "Earliest";

    public bool EnableAutoCommit { get; set; } = false;

    public int FetchMinBytes { get; set; } = 32768;

    public int FetchWaitMaxMs { get; set; } = 100;
}


