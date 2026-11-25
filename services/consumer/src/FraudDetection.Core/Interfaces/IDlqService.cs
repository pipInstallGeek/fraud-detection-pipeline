namespace FraudDetection.Core.Interfaces;


public interface IDlqService 
{
      Task SendToDlqAsync(
        byte[] payload, 
        string reason, 
        string? sourceTopic = null, 
        int? partition = null, 
        long? offset = null, 
        CancellationToken cancellationToken = default);
}