# Multi-stage build for C# Fraud Detection Consumer with ONNX Runtime
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# 1. Copy project files for optimal Docker layer caching
COPY ["src/FraudDetection.Core/FraudDetection.Core.csproj", "src/FraudDetection.Core/"]
COPY ["src/FraudDetection.Infrastructure/FraudDetection.Infrastructure.csproj", "src/FraudDetection.Infrastructure/"]
COPY ["src/FraudDetection.Consumer/FraudDetection.Consumer.csproj", "src/FraudDetection.Consumer/"]

# 2. Restore NuGet dependencies
RUN dotnet restore "src/FraudDetection.Consumer/FraudDetection.Consumer.csproj"

# 3. Copy source code and publish release build
COPY src/ src/
WORKDIR "/src/src/FraudDetection.Consumer"
RUN dotnet publish "FraudDetection.Consumer.csproj" -c Release -o /app/publish /p:UseAppHost=false

# 4. Final runtime image (Debian bookworm-slim with native glibc for libonnxruntime)
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "FraudDetection.Consumer.dll"]
