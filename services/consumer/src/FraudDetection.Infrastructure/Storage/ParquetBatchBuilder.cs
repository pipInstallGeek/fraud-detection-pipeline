namespace FraudDetection.Infrastructure.Storage;

using System.IO;
using FraudDetection.Core.Models;
using Parquet.Serialization;


public class ParquetBatchBuilder
{

    public static async Task<Stream> BuildParquetStreamAsync(IReadOnlyList<Transaction> transactions, CancellationToken cancellationToken = default)
    {
        var memoryStream = new MemoryStream();

        await ParquetSerializer.SerializeAsync<Transaction>(transactions, memoryStream, cancellationToken: cancellationToken);

        memoryStream.Position = 0;

        return memoryStream;
    }
}