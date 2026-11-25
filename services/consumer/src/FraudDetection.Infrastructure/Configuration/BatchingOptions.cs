namespace FraudDetection.Infrastructure.Configuration;

public class BatchingOptions
{
   public const string SectionName = "Batching";

    
    public int MaxBatchSize { get; set; } = 5000;

    
    public int FlushIntervalSeconds { get; set; } = 30;

    
    public int ChannelCapacity { get; set; } = 50000; 
}