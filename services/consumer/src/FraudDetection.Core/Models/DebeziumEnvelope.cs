
namespace FraudDetection.Core.Models;

using System.Text.Json.Serialization;


public record DebeziumEnvelope<T> 
{
    public T? Before  { get; init; }
    
    public T? After  { get; init; }

    public string Op  { get; init; } = string.Empty;
    
    [JsonPropertyName("ts_ms")]
    public long? TimestampMs  { get; init; }
}