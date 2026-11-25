namespace FraudDetection.Core.Interfaces;

using FraudDetection.Core.Models;

public record FraudPrediction(bool IsFraud, float Probability);

public interface IFraudPredictor
{
    FraudPrediction Predict(Transaction tx);
}
