namespace FraudDetection.Core.Interfaces;


using FraudDetection.Core.Models;

public interface ILakehouseWriter
{

    Task WriteBatchAsync(IReadOnlyList<Transaction> transactions, CancellationToken cancellationToken = default);

}