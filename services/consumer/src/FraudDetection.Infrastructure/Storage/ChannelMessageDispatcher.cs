namespace FraudDetection.Infrastructure.Storage;

using System.Threading.Channels;
using FraudDetection.Core.Interfaces;
using FraudDetection.Core.Models;
using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Options;


public class ChannelMessageDispatcher : IMessageDispatcher
{

    private readonly BatchingOptions _options;
    private readonly Channel<Transaction> _channel;


    public ChannelMessageDispatcher(IOptions<BatchingOptions> options)
    {
        _options = options.Value;

        var channelOptions = new BoundedChannelOptions(_options.ChannelCapacity){};

        _channel = Channel.CreateBounded<Transaction>(channelOptions);
    }

    

    public ValueTask DispatchAsync(Transaction transaction, CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(transaction, cancellationToken);
    }

    public IAsyncEnumerable<Transaction> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }

    public void Complete()
    {
        _channel.Writer.TryComplete();
    }
}