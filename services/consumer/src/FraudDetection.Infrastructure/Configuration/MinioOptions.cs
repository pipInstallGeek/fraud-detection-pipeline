namespace FraudDetection.Infrastructure.Configuration;


public class MinioOptions 
{
    public const string SectionName = "MinIO";

  
    public string Endpoint { get; set; } = "http://localhost:9000";

    
    public string AccessKey { get; set; } = "minioadmin";

    
    public string SecretKey { get; set; } = "minioadmin";

    
    public string BucketName { get; set; } = "lakehouse";

    
    public string BronzePathPrefix { get; set; } = "bronze/transactions";
}