namespace FraudDetection.Infrastructure.Kafka;

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FraudDetection.Core.Interfaces;
using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class KafkaDlqService : IDlqService, IDisposable
{
    private readonly IProducer<Null, byte[]> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaDlqService> _logger;

    public KafkaDlqService(IOptions<KafkaOptions> options, ILogger<KafkaDlqService> logger)
    {
        _options = options.Value;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            Acks = Acks.Leader,
            MessageTimeoutMs = 5000,
            LingerMs = 5
        };

        _producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
    }

    public async Task SendToDlqAsync(
        byte[] payload,
        string reason,
        string? sourceTopic = null,
        int? partition = null,
        long? offset = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var headers = new Headers
            {
                { "x-error-reason", Encoding.UTF8.GetBytes(reason) },
                { "x-original-topic", Encoding.UTF8.GetBytes(sourceTopic ?? string.Empty) },
                { "x-original-partition", Encoding.UTF8.GetBytes(partition?.ToString() ?? "-1") },
                { "x-original-offset", Encoding.UTF8.GetBytes(offset?.ToString() ?? "-1") },
                { "x-failed-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) }
            };

            var message = new Message<Null, byte[]>
            {
                Value = payload,
                Headers = headers
            };

            var deliveryReport = await _producer.ProduceAsync(_options.TopicDlq, message, cancellationToken);

            _logger.LogWarning(
                "[DLQ_ROUTED] Offset {Offset} from topic '{SourceTopic}' routed to DLQ '{DlqTopic}' (Partition: {DlqPartition}, Offset: {DlqOffset}). Reason: {Reason}",
                offset, sourceTopic, _options.TopicDlq, deliveryReport.Partition.Value, deliveryReport.Offset.Value, reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message to DLQ topic '{DlqTopic}'", _options.TopicDlq);
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        GC.SuppressFinalize(this);
    }
}
