namespace FraudDetection.Core.Models;

public record Transaction
{
    public int Id { get; init; }
    public int Step { get; init; }
    public string Type { get; init; } = string.Empty;

    public string Amount { get; init; } = string.Empty;
    public string NameOrig { get; init; } = string.Empty;
    public string OldBalanceOrig { get; init; } = string.Empty;
    public string NewBalanceOrig { get; init; } = string.Empty;
    public string NameDest { get; init; } = string.Empty;
    public string OldBalanceDest { get; init; } = string.Empty;
    public string NewBalanceDest { get; init; } = string.Empty;

    public int IsFraud { get; init; }
    public int IsFlaggedFraud { get; init; }
}