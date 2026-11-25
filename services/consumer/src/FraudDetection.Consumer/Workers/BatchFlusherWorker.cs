using FraudDetection.Core.Interfaces;
using FraudDetection.Core.Models;
using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Options;

namespace FraudDetection.Consumer.Workers;



public class BatchFlusherWorker : BackgroundService
{

    private readonly IMessageDispatcher _dispatcher;
    private readonly ILakehouseWriter _writer;
    private readonly BatchingOptions _options;
    private readonly ILogger<BatchFlusherWorker> _logger;

    private readonly SemaphoreSlim _lock = new(1,1);
    private readonly List<Transaction> _batch = [];

    public BatchFlusherWorker(IMessageDispatcher dispatcher, ILakehouseWriter writer, IOptions<BatchingOptions> options,ILogger<BatchFlusherWorker> logger)
    {
        _dispatcher = dispatcher;
        _writer = writer;
        _options = options.Value;
        _logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {

        var timerTask = StartPeriodicFlushTimerAsync(stoppingToken);
        var consumerTask = StartChannelConsumerAsync(stoppingToken);

        await Task.WhenAll(timerTask, consumerTask);

        await FlushBatchInternalAsync(CancellationToken.None);
    }

    private async Task StartChannelConsumerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach(var transaction in _dispatcher.ReadAllAsync(cancellationToken))
            {
                bool shouldFlush = false;
                await _lock.WaitAsync(cancellationToken);
                try
                {
                    _batch.Add(transaction);
                    if(_batch.Count >= _options.MaxBatchSize)
                    {
                        shouldFlush = true;
                    }
                }
                finally
                {
                    _lock.Release();
                }

                if(shouldFlush)
                {
                    await FlushBatchInternalAsync(cancellationToken);
                }
            }
        }
        catch(OperationCanceledException)
        {
            _logger.LogInformation("Kafka poller is stopping gracefully...");
        }
        catch(Exception ex)
        {
            _logger.LogError(ex, "Unexpected error");
        }
    }

    private async Task StartPeriodicFlushTimerAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.FlushIntervalSeconds));

        try
        {
            while(await timer.WaitForNextTickAsync(cancellationToken))
            {
                await FlushBatchInternalAsync(cancellationToken);
            }
        }
        catch(OperationCanceledException)
        {
            _logger.LogInformation("Kafka poller is stopping gracefully...");
        }
    }

    private async Task FlushBatchInternalAsync(CancellationToken cancellationToken)
    {
        List<Transaction> copy;

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if(_batch.Count == 0 ) return;
            copy = [.. _batch];
            _batch.Clear();
        }
        finally
        {
            _lock.Release();
        }

        await _writer.WriteBatchAsync(copy, cancellationToken);
    }
}