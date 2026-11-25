namespace FraudDetection.Core.Interfaces;

using FraudDetection.Core.Models;


public interface IMessageDispatcher 
{
    ValueTask DispatchAsync(Transaction transaction, CancellationToken cancellationToken = default);

    IAsyncEnumerable<Transaction> ReadAllAsync(CancellationToken cancellationToken = default);
    
    void Complete();

}