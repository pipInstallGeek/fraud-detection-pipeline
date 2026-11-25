namespace FraudDetection.Infrastructure.Storage;

using Amazon.S3;
using Amazon.S3.Model;
using FraudDetection.Core.Interfaces;
using FraudDetection.Core.Models;
using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;


public class MinioParquetWriter : ILakehouseWriter
{

    private readonly MinioOptions _options;
    private readonly AmazonS3Client _s3Client;
    private readonly ILogger<MinioParquetWriter> _logger;
    private bool _bucketChecked;

    public MinioParquetWriter(IOptions<MinioOptions> options, ILogger<MinioParquetWriter> logger)
    {
        _options = options.Value;
        _logger = logger;

        var config = new AmazonS3Config
        {
            ServiceURL = _options.Endpoint,
            ForcePathStyle = true
        };

        _s3Client =  new AmazonS3Client(_options.AccessKey, _options.SecretKey, config);
    }

    private async Task EnsureBucketExistsAsync(CancellationToken cancellationToken)
    {
        if (_bucketChecked) return;

        try
        {
            await _s3Client.PutBucketAsync(new PutBucketRequest { BucketName = _options.BucketName }, cancellationToken);
            _logger.LogInformation("Ensured bucket '{BucketName}' exists in MinIO.", _options.BucketName);
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
        {
            // Normal: bucket already exists
        }

        _bucketChecked = true;
    }

    public async Task WriteBatchAsync(IReadOnlyList<Transaction> transactions, CancellationToken cancellationToken = default)
    {
        if(transactions.Count == 0) return;

        await EnsureBucketExistsAsync(cancellationToken);

        await using var stream = await ParquetBatchBuilder.BuildParquetStreamAsync(transactions, cancellationToken);

        var key = $"{_options.BronzePathPrefix}/batch_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid():N}.parquet";

        var putRequest = new PutObjectRequest
        {
            BucketName = _options.BucketName,
            Key = key,
            InputStream = stream
        };

        await _s3Client.PutObjectAsync(putRequest, cancellationToken);
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Uploaded {Count} transactions to MinIO: {Key}", transactions.Count, key);
        }
    }
}