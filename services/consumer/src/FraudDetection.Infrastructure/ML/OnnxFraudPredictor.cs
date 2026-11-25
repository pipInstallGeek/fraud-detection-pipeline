namespace FraudDetection.Infrastructure.ML;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading;
using FraudDetection.Core.Interfaces;
using FraudDetection.Core.Models;
using FraudDetection.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;

public class OnnxFraudPredictor : IFraudPredictor, IDisposable
{
    private InferenceSession? _session;
    private readonly SessionOptions _sessionOptions;
    private readonly FileSystemWatcher? _watcher;
    private readonly float _threshold;
    private readonly ILogger<OnnxFraudPredictor> _logger;

    public OnnxFraudPredictor(IOptions<ModelOptions> options, ILogger<OnnxFraudPredictor> logger)
    {
        _logger = logger;
        _threshold = options.Value.Threshold;
        var modelPath = options.Value.ModelPath;

        var fullPath = Path.IsPathRooted(modelPath)
            ? modelPath
            : Path.Combine(AppContext.BaseDirectory, modelPath);

        _sessionOptions = new SessionOptions
        {
            ExecutionMode = ExecutionMode.ORT_SEQUENTIAL,
            IntraOpNumThreads = 1,
            InterOpNumThreads = 1
        };

        if (File.Exists(fullPath))
        {
            _session = new InferenceSession(fullPath, _sessionOptions);
            _logger.LogInformation("Loaded active ONNX fraud model from '{Path}'", fullPath);
        }
        else
        {
            _session = null;
            _logger.LogWarning("ONNX model file not found at '{Path}'. Consumer starting in standby mode (monitoring directory for model creation)...", fullPath);
        }

        var modelDir = Path.GetDirectoryName(fullPath);
        var modelFileName = Path.GetFileName(fullPath);
        if (!string.IsNullOrEmpty(modelDir))
        {
            if (!Directory.Exists(modelDir))
            {
                Directory.CreateDirectory(modelDir);
            }

            try
            {
                _watcher = new FileSystemWatcher(modelDir, modelFileName)
                {
                    NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.CreationTime | NotifyFilters.FileName,
                    EnableRaisingEvents = true
                };
                _watcher.Changed += OnModelFileUpdated;
                _watcher.Created += OnModelFileUpdated;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not initialize model hot-reload watcher on '{Dir}'", modelDir);
            }
        }
    }

    private void OnModelFileUpdated(object sender, FileSystemEventArgs e)
    {
        try
        {
            Thread.Sleep(300);
            _logger.LogInformation("Detected promoted ONNX model file at '{Path}'. Hot-reloading session in memory...", e.FullPath);

            var newSession = new InferenceSession(e.FullPath, _sessionOptions);
            var oldSession = Interlocked.Exchange(ref _session, newSession);
            oldSession?.Dispose();

            _logger.LogInformation("Successfully hot-reloaded new ONNX champion model with zero downtime.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed hot-reloading updated ONNX model. Retaining existing active session.");
        }
    }

    public FraudPrediction Predict(Transaction tx)
    {
        var amount = DecodeDecimal(tx.Amount);
        var oldOrig = DecodeDecimal(tx.OldBalanceOrig);
        var newOrig = DecodeDecimal(tx.NewBalanceOrig);
        var oldDest = DecodeDecimal(tx.OldBalanceDest);
        var newDest = DecodeDecimal(tx.NewBalanceDest);

        var tensor = new DenseTensor<float>([1, 10]);

        tensor[0, 0] = (float)tx.Step;
        tensor[0, 1] = MapTypeToCode(tx.Type);
        tensor[0, 2] = amount;
        tensor[0, 3] = oldOrig;
        tensor[0, 4] = newOrig;
        tensor[0, 5] = oldDest;
        tensor[0, 6] = newDest;
        tensor[0, 7] = newOrig + amount - oldOrig;
        tensor[0, 8] = oldDest + amount - newDest;
        tensor[0, 9] = (newOrig == 0f && amount > 0f) ? 1.0f : 0.0f;

        var input = NamedOnnxValue.CreateFromTensor("float_input", tensor);
        
        var currentSession = _session;
        if (currentSession == null)
        {
            return new FraudPrediction(false, 0f);
        }

        using var result = currentSession.Run([input]);

        var probabilities = result.First(r => r.Name == "probabilities").AsTensor<float>();
        var fraudProbability = probabilities[0, 1];
        var isFraud = fraudProbability >= _threshold;

        return new FraudPrediction(isFraud, fraudProbability);
    }

    private static float DecodeDecimal(string? base64)
    {
        if (string.IsNullOrWhiteSpace(base64)) return 0f;

        try
        {
            Span<byte> bytes = stackalloc byte[32];
            if (Convert.TryFromBase64String(base64, bytes, out int bytesWritten) && bytesWritten > 0)
            {
                var bigInt = new BigInteger(bytes.Slice(0, bytesWritten), isUnsigned: false, isBigEndian: true);
                return (float)((double)bigInt / 100.0);
            }
        }
        catch
        {
            // fallback
        }

        if (float.TryParse(base64, CultureInfo.InvariantCulture, out float val))
        {
            return val;
        }

        return 0f;
    }

    private static float MapTypeToCode(string? type)
    {
        return type?.ToUpperInvariant() switch
        {
            "PAYMENT" => 0.0f,
            "TRANSFER" => 1.0f,
            "CASH_OUT" => 2.0f,
            "DEBIT" => 3.0f,
            "CASH_IN" => 4.0f,
            _ => 0.0f
        };
    }

    public void Dispose()
    {
        _watcher?.Dispose();
        _session?.Dispose();
        _sessionOptions?.Dispose();
        GC.SuppressFinalize(this);
    }
}
