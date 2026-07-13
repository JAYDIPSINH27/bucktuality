using System.Text.Json;
using Confluent.Kafka;

namespace Bucktuality.ModerationService.Services;

public sealed class KafkaProducerService : IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(
        IConfiguration configuration,
        ILogger<KafkaProducerService> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers =
                configuration["Kafka:BootstrapServers"] ?? "kafka:9092",

            MessageTimeoutMs = 5000,
            SocketTimeoutMs = 5000
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task PublishAsync<T>(
        string topic,
        T eventData,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var json = JsonSerializer.Serialize(eventData);

            await _producer.ProduceAsync(
                topic,
                new Message<Null, string>
                {
                    Value = json
                },
                cancellationToken);

            _logger.LogInformation(
                "Published Kafka event to topic {Topic}",
                topic);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Kafka event could not be published to topic {Topic}. " +
                "The primary database operation was still completed.",
                topic);
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(3));
        _producer.Dispose();
    }
}