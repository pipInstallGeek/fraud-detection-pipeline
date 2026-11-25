namespace FraudDetection.Consumer.Workers;

using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FraudDetection.Core.Interfaces;
using FraudDetection.Infrastructure.Configuration;
using FraudDetection.Infrastructure.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class KafkaPollerWorker : BackgroundService
{
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly IMessageDispatcher _dispatcher;
    private readonly IFraudPredictor _fraudPredictor;
    private readonly IDlqService _dlqService;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaPollerWorker> _logger;

    private long _totalConsumed;
    private long _fraudAlertCount;
    private long _dlqCount;

    public KafkaPollerWorker(
        KafkaConsumerFactory consumerFactory,
        IMessageDispatcher dispatcher,
        IFraudPredictor fraudPredictor,
        IDlqService dlqService,
        IOptions<KafkaOptions> options,
        ILogger<KafkaPollerWorker> logger)
    {
        _consumerFactory = consumerFactory;
        _dispatcher = dispatcher;
        _fraudPredictor = fraudPredictor;
        _dlqService = dlqService;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Run(async () =>
        {
            using var consumer = _consumerFactory.CreateRawConsumer();
            consumer.Subscribe(_options.Topic);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(
                    "Subscribed to topic '{Topic}'. DLQ enabled on '{DlqTopic}'. Starting real-time ML inference & polling loop...",
                    _options.Topic, _options.TopicDlq);
            }

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);
                        if (result?.Message?.Value != null)
                        {
                            bool isSuccess = false;

                            try
                            {
                                var transaction = DebeziumDeserializer.Deserialize(result.Message.Value);
                                if (transaction == null)
                                {
                                    _dlqCount++;
                                    _logger.LogWarning("Deserialization returned null for offset {Offset}. Routing to DLQ.", result.Offset.Value);

                                    await _dlqService.SendToDlqAsync(
                                        result.Message.Value,
                                        "Deserialization returned null payload",
                                        result.Topic,
                                        result.Partition.Value,
                                        result.Offset.Value,
                                        stoppingToken);
                                }
                                else
                                {
                                    _totalConsumed++;

                                    var prediction = _fraudPredictor.Predict(transaction);
                                    if (prediction.IsFraud)
                                    {
                                        _fraudAlertCount++;
                                        _logger.LogWarning(
                                            "[FRAUD_ALERT #{Count}] Tx ID: {Id} | Step: {Step} | Type: {Type} | Fraud Prob: {Prob:P2} | Ground Truth: {GroundTruth}",
                                            _fraudAlertCount, transaction.Id, transaction.Step, transaction.Type, prediction.Probability, transaction.IsFraud);
                                    }

                                    if (_totalConsumed % 50000 == 0)
                                    {
                                        _logger.LogInformation("Processed {Total:N0} live transactions in memory. Flagged {Alerts:N0} frauds. DLQ: {Dlq:N0}",
                                            _totalConsumed, _fraudAlertCount, _dlqCount);
                                    }

                                    await _dispatcher.DispatchAsync(transaction, stoppingToken);
                                    isSuccess = true;
                                }
                            }
                            catch (Exception ex)
                            {
                                _dlqCount++;
                                _logger.LogError(ex, "Failed processing message at offset {Offset}. Routing to DLQ.", result.Offset.Value);

                                await _dlqService.SendToDlqAsync(
                                    result.Message.Value,
                                    $"Processing exception: {ex.Message}",
                                    result.Topic,
                                    result.Partition.Value,
                                    result.Offset.Value,
                                    stoppingToken);
                            }
                            finally
                            {
                                if (!isSuccess || _totalConsumed % 1000 == 0)
                                {
                                    consumer.Commit(result);
                                }
                            }
                        }
                    }
                    catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                    {
                        if (_logger.IsEnabled(LogLevel.Warning))
                        {
                            _logger.LogWarning(
                                "Topic '{Topic}' is not yet available in Kafka ({Reason}). Retrying in 5s...",
                                _options.Topic, ex.Error.Reason);
                        }

                        await Task.Delay(5000, stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        if (_logger.IsEnabled(LogLevel.Warning))
                        {
                            _logger.LogWarning("Transient Kafka consume warning: {Reason}. Retrying in 2s...", ex.Error.Reason);
                        }
                        await Task.Delay(2000, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation(
                    "Kafka poller stopping gracefully. Total processed: {Total}, Frauds flagged: {Alerts}, DLQ: {Dlq}",
                    _totalConsumed, _fraudAlertCount, _dlqCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in Kafka poller worker.");
            }
            finally
            {
                try
                {
                    consumer.Commit();
                }
                catch
                {
                    // Ignore on shutdown
                }

                consumer.Close();
                _dispatcher.Complete();
            }
        }, stoppingToken);
    }
}
