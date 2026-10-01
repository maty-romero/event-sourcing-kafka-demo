using Microsoft.Extensions.Logging;
using Xunit;

namespace TransactionsAPI.Tests;

public class FakeEventStore : IEventStore
{
    public List<StoredEvent> Events { get; } = new();
    public long? ExpectedVersionOnAppend { get; private set; }
    public bool Exists { get; set; }

    // Cuantas veces debe devolver WrongExpectedVersion antes de dejar pasar.
    public int ConflictsToSimulate { get; set; }

    public int AppendCalls { get; private set; }
    public int ReadCalls { get; private set; }

    public Task<List<StoredEvent>?> ReadStreamAsync(string stream, CancellationToken ct = default)
    {
        ReadCalls++;
        return Task.FromResult(Exists ? Events : (List<StoredEvent>?)null);
    }

    public Task<AppendResult> AppendAsync(
        string stream, long expectedVersion, PendingEvent pendingEvent, CancellationToken ct = default)
    {
        AppendCalls++;
        ExpectedVersionOnAppend = expectedVersion;

        if (ConflictsToSimulate > 0)
        {
            ConflictsToSimulate--;
            // El "otro escritor" appendeo un deposito de 1: reescribi la foto
            // del estado para que el proximo read la vea.
            Events.Add(new StoredEvent(
                Events.Count, "other-writer", AccountEventTypes.MoneyDeposited, 1, "2026-10-01T00:00:00Z"));
            Exists = true;
            return Task.FromResult(AppendResult.WrongExpectedVersion);
        }

        if (expectedVersion == -1)
        {
            if (Exists)
                return Task.FromResult(AppendResult.WrongExpectedVersion);
            Exists = true;
            Events.Clear();
        }
        else if (!Exists || expectedVersion != Events.Count - 1)
        {
            return Task.FromResult(AppendResult.WrongExpectedVersion);
        }

        Events.Add(new StoredEvent(
            Events.Count, pendingEvent.EventId, pendingEvent.EventType,
            pendingEvent.Amount, pendingEvent.Timestamp));
        return Task.FromResult(AppendResult.Success);
    }
}

public class AccountStoreTests
{
    private static PendingEvent Pending(string type, int amount) =>
        new(Guid.NewGuid().ToString(), type, "901", amount, "2026-10-01T00:00:00Z");

    [Fact]
    public async Task CreateAppendeaConNoStream()
    {
        var es = new FakeEventStore();
        var store = new AccountStore(es, Logger());

        var (balance, revision) = await store.CreateAsync("901", CancellationToken.None);

        Assert.Equal(0, balance);
        Assert.Equal(0, revision);
        Assert.Equal(-1, es.ExpectedVersionOnAppend);
        Assert.Single(es.Events);
    }

    [Fact]
    public async Task CreateDuplicadoLanzaAlreadyExists()
    {
        var es = new FakeEventStore { Exists = true };
        var store = new AccountStore(es, Logger());

        await Assert.ThrowsAsync<AccountAlreadyExistsException>(() =>
            store.CreateAsync("901", CancellationToken.None));
    }

    [Fact]
    public async Task DepositSobreStreamInexistenteLanzaNotFound()
    {
        var es = new FakeEventStore();
        var store = new AccountStore(es, Logger());

        await Assert.ThrowsAsync<AccountNotFoundException>(() =>
            store.DepositAsync("901", 10, CancellationToken.None));
    }

    [Fact]
    public async Task AppendConcurrencteSeReintentaContraElEstadoReal()
    {
        // El stream tiene 2 eventos; el primer append intenta con revision 1 y
        // recibe WrongExpectedVersion (otro escritor gano). El store debe
        // re-leer, re-validar contra el estado nuevo y appendear con la
        // revision correcta.
        var es = new FakeEventStore { Exists = true, ConflictsToSimulate = 1 };
        es.Events.Add(new StoredEvent(0, "e0", AccountEventTypes.AccountCreated, 0, "t"));
        es.Events.Add(new StoredEvent(1, "e1", AccountEventTypes.MoneyDeposited, 100, "t"));
        var store = new AccountStore(es, Logger());

        var (balance, revision) = await store.DepositAsync("901", 50, CancellationToken.None);

        Assert.Equal(2, es.AppendCalls);
        Assert.Equal(2, es.ReadCalls);
        // Tras el conflicto el fake agrego un deposito de 1: balance 100+1+50.
        Assert.Equal(151, balance);
        Assert.Equal(3, revision);
        Assert.Equal(2, es.ExpectedVersionOnAppend);
    }

    [Fact]
    public async Task RetiroValidoAppendeaConLaRevisionLeida()
    {
        var es = new FakeEventStore { Exists = true };
        es.Events.Add(new StoredEvent(0, "e0", AccountEventTypes.AccountCreated, 0, "t"));
        es.Events.Add(new StoredEvent(1, "e1", AccountEventTypes.MoneyDeposited, 100, "t"));
        var store = new AccountStore(es, Logger());

        var (balance, revision) = await store.WithdrawAsync("901", 100, CancellationToken.None);

        Assert.Equal(0, balance);
        Assert.Equal(2, revision);
        Assert.Equal(1, es.ExpectedVersionOnAppend);
    }

    [Fact]
    public async Task RetiroSinSaldoNuncaAppendea()
    {
        // Limitacion 2c de la V1 resuelta: el evento invalido no nace,
        // no hay nada que descartar aguas abajo.
        var es = new FakeEventStore { Exists = true };
        es.Events.Add(new StoredEvent(0, "e0", AccountEventTypes.AccountCreated, 0, "t"));
        es.Events.Add(new StoredEvent(1, "e1", AccountEventTypes.MoneyDeposited, 10, "t"));
        var store = new AccountStore(es, Logger());

        await Assert.ThrowsAsync<InsufficientFundsException>(() =>
            store.WithdrawAsync("901", 100, CancellationToken.None));

        Assert.Equal(0, es.AppendCalls);
    }

    private static ILogger<AccountStore> Logger() =>
        Microsoft.Extensions.Logging.Abstractions.NullLogger<AccountStore>.Instance;
}
