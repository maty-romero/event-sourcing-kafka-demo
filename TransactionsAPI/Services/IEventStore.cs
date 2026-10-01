public enum AppendResult
{
    Success,
    WrongExpectedVersion
}

public interface IEventStore
{
    Task<List<StoredEvent>?> ReadStreamAsync(string stream, CancellationToken ct = default);

    Task<AppendResult> AppendAsync(
        string stream, long expectedVersion, PendingEvent pendingEvent, CancellationToken ct = default);
}
