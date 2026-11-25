using FraudDetection.Consumer;
using FraudDetection.Consumer.Workers;
using FraudDetection.Core.Interfaces;
using FraudDetection.Infrastructure.Configuration;
using FraudDetection.Infrastructure.Kafka;
using FraudDetection.Infrastructure.ML;
using FraudDetection.Infrastructure.Storage;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<MinioOptions>(builder.Configuration.GetSection(MinioOptions.SectionName));
builder.Services.Configure<BatchingOptions>(builder.Configuration.GetSection(BatchingOptions.SectionName));
builder.Services.Configure<ModelOptions>(builder.Configuration.GetSection(ModelOptions.SectionName));

builder.Services.AddSingleton<KafkaConsumerFactory>();
builder.Services.AddSingleton<IMessageDispatcher, ChannelMessageDispatcher>();
builder.Services.AddSingleton<ILakehouseWriter, MinioParquetWriter>();
builder.Services.AddSingleton<IFraudPredictor, OnnxFraudPredictor>();

builder.Services.AddHostedService<KafkaPollerWorker>();
builder.Services.AddHostedService<BatchFlusherWorker>();

builder.Services.AddSingleton<IDlqService, KafkaDlqService>();

var host = builder.Build();
host.Run();
