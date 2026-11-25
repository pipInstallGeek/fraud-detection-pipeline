namespace FraudDetection.Infrastructure.Kafka;

using System.Text.Json;
using System.Text.Json.Serialization;
using FraudDetection.Core.Models;

public class DebeziumDeserializer
{

    private static readonly JsonSerializerOptions options = new() 
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };

    public static Transaction? Deserialize(ReadOnlySpan<byte> utf8JsonBytes)
    {
        var result = JsonSerializer.Deserialize<DebeziumEnvelope<Transaction>>(utf8JsonBytes, options);

        return result?.After;
    }

}   