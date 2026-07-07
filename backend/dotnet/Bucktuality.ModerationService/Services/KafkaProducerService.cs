using System.Text.Json;
using Confluent.Kafka;

namespace Bucktuality.ModerationService.Services;

public class KafkaProducerService
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
            BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "kafka:9092"
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task PublishAsync<T>(string topic, T eventData)
    {
        try
        {
            var json = JsonSerializer.Serialize(eventData);

            await _producer.ProduceAsync(topic, new Message<Null, string>
            {
                Value = json
            });

            _logger.LogInformation("Published event to {Topic}: {Event}", topic, json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish event to topic {Topic}", topic);
        }
    }
}