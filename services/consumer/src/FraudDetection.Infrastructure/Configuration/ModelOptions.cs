namespace FraudDetection.Infrastructure.Configuration;

public class ModelOptions
{
    public const string SectionName = "Model";

    public string ModelPath { get; set; } = "models/fraud_model.onnx";
    public float Threshold { get; set; } = 0.50f;
}
