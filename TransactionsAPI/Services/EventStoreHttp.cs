using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

public sealed class EventStoreException(string message) : Exception(message);

public sealed class EventStoreHttp : IEventStore
{
    // Contrato verificado contra EventStoreDB 23.10 (API HTTP/JSON, sin gRPC):
    //  - append:    POST /streams/{s} + header ES-ExpectedVersion. 201 ok, 400 = WrongExpectedVersion.
    //  - lectura:   GET /streams/{s}/head/{n}?embed=content. 404 = stream inexistente.
    //               El envelope trae entries en orden descendente; aqui se invierten.
    private const string EventsMediaType = "application/vnd.eventstore.events+json";
    private const int ReadMaxCount = 1000;

    private readonly HttpClient _http;
    private readonly ILogger<EventStoreHttp> _logger;

    public EventStoreHttp(HttpClient http, IOptions<EventStoreOptions> options, ILogger<EventStoreHttp> logger)
    {
        _http = http;
        _http.BaseAddress = new Uri(options.Value.Url);
        _logger = logger;
    }

    public async Task<AppendResult> AppendAsync(
        string stream, long expectedVersion, PendingEvent pendingEvent, CancellationToken ct = default)
    {
        var body = new[]
        {
            new
            {
                eventId = pendingEvent.EventId,
                eventType = pendingEvent.EventType,
                data = new { amount = pendingEvent.Amount },
                metadata = new { timestamp = pendingEvent.Timestamp, accountId = pendingEvent.AccountId }
            }
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, $"/streams/{Uri.EscapeDataString(stream)}");
        request.Headers.TryAddWithoutValidation("ES-ExpectedVersion", expectedVersion.ToString());
        request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, EventsMediaType);

        using var response = await _http.SendAsync(request, ct);

        switch (response.StatusCode)
        {
            case HttpStatusCode.Created:
                return AppendResult.Success;
            case HttpStatusCode.BadRequest or HttpStatusCode.Gone:
                return AppendResult.WrongExpectedVersion;
            default:
                var error = await response.Content.ReadAsStringAsync(ct);
                _logger.LogError("append a {Stream} fallo: {Status} {Error}", stream, response.StatusCode, error);
                throw new EventStoreException($"append a {stream}: {(int)response.StatusCode} {error}");
        }
    }

    public async Task<List<StoredEvent>?> ReadStreamAsync(string stream, CancellationToken ct = default)
    {
        var url = $"/streams/{Uri.EscapeDataString(stream)}/head/{ReadMaxCount}?embed=content";
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("Accept", EventsMediaType);

        using var response = await _http.SendAsync(request, ct);

        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;
        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(ct);
            throw new EventStoreException($"lectura de {stream}: {(int)response.StatusCode} {error}");
        }

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        var events = new List<StoredEvent>();

        if (doc.RootElement.TryGetProperty("entries", out var entries) && entries.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in entries.EnumerateArray())
            {
                var content = entry.GetProperty("content");
                events.Add(new StoredEvent(
                    content.GetProperty("eventNumber").GetInt64(),
                    content.GetProperty("eventId").GetString() ?? "",
                    content.GetProperty("eventType").GetString() ?? "",
                    content.TryGetProperty("data", out var data)
                        && data.TryGetProperty("amount", out var amount) ? amount.GetInt32() : 0,
                    ReadTimestamp(content)));
            }
        }

        events.Sort((a, b) => a.EventNumber.CompareTo(b.EventNumber));
        return events;
    }

    private static string ReadTimestamp(JsonElement content)
    {
        if (!content.TryGetProperty("metadata", out var metadata))
            return "";

        // metadata puede venir como objeto ({timestamp:...}) o como string vacio ("")
        if (metadata.ValueKind == JsonValueKind.Object
            && metadata.TryGetProperty("timestamp", out var timestamp))
            return timestamp.GetString() ?? "";

        return "";
    }
}
