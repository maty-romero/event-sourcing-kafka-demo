using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

public record PublishedEvent
{
    public required string EventType { get; set; }
    public required string AccountId { get; set; }
    public required string Timestamp { get; set; }
    public required int Amount { get; set; }
}

public interface IPublisher
{
    Task Publish(string eventType, string accountId, int? amount);
}

public class KafkaPublisher : IPublisher
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaPublisher> _logger;

    public KafkaPublisher(IOptions<KafkaOptions> options, ILogger<KafkaPublisher> logger)
    {
        _logger = logger;
        _topic = options.Value.Topic;

        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task Publish(string eventType, string stringAccountId, int? amount)
    {
        var evt = new PublishedEvent
        {
            EventType = eventType,
            AccountId = stringAccountId,
            Timestamp = DateTime.UtcNow.ToString("O"),
            Amount = amount ?? 0
        };

        var json = JsonSerializer.Serialize(evt);

        var message = new Message<string, string>
        {
            Key = stringAccountId,
            Value = json
        };

        try
        {
            var result = await _producer.ProduceAsync(_topic, message);

            _logger.LogInformation(
                "Event {EventType} published to {Topic} [partition: {Partition}, offset: {Offset}]",
                eventType, _topic, result.Partition.Value, result.Offset.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Failed to publish event {EventType} to {Topic}: {Reason}",
                eventType, _topic, ex.Error.Reason);
            throw;
        }
    }
}
