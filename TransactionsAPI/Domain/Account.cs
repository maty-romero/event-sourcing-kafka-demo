public static class AccountEventTypes
{
    public const string AccountCreated = "AccountCreated";
    public const string MoneyDeposited = "MoneyDeposited";
    public const string MoneyWithdrawn = "MoneyWithdrawn";
}

public sealed record PendingEvent(
    string EventId,
    string EventType,
    string AccountId,
    int Amount,
    string Timestamp);

public sealed class Account
{
    public string Id { get; }
    public int Balance { get; private set; }

    // -1: el stream todavia no existe (la cuenta no nacio).
    // >= 0: numero del ultimo evento aplicado = revision del stream.
    public long Version { get; private set; } = -1;

    public bool Exists => Version >= 0;

    public Account(string id) => Id = id;

    public static PendingEvent NewCreatedEvent(string accountId) =>
        new(Guid.NewGuid().ToString(), AccountEventTypes.AccountCreated, accountId, 0,
            DateTime.UtcNow.ToString("O"));

    public static Account Fold(string id, IReadOnlyList<StoredEvent> events)
    {
        var account = new Account(id);
        foreach (var stored in events)
            account.Apply(stored);
        return account;
    }

    private void Apply(StoredEvent stored)
    {
        switch (stored.EventType)
        {
            case AccountEventTypes.AccountCreated:
                Balance = 0;
                break;
            case AccountEventTypes.MoneyDeposited:
                Balance += stored.Amount;
                break;
            case AccountEventTypes.MoneyWithdrawn:
                Balance -= stored.Amount;
                break;
            default:
                throw new UnknownEventException(stored.EventType);
        }
        Version = stored.EventNumber;
    }

    public PendingEvent Deposit(int amount)
    {
        EnsureOpen();
        EnsurePositive(amount);
        return New(AccountEventTypes.MoneyDeposited, amount);
    }

    public PendingEvent Withdraw(int amount)
    {
        EnsureOpen();
        EnsurePositive(amount);
        if (Balance - amount < 0)
            throw new InsufficientFundsException(Id, Balance, amount);
        return New(AccountEventTypes.MoneyWithdrawn, amount);
    }

    private void EnsureOpen()
    {
        if (!Exists)
            throw new AccountNotFoundException(Id);
    }

    private static void EnsurePositive(int amount)
    {
        if (amount <= 0)
            throw new InvalidAmountException(amount);
    }

    private PendingEvent New(string eventType, int amount) =>
        new(Guid.NewGuid().ToString(), eventType, Id, amount,
            DateTime.UtcNow.ToString("O"));
}

public sealed class AccountNotFoundException(string id)
    : Exception($"Account {id} not found");

public sealed class AccountAlreadyExistsException(string id)
    : Exception($"Account {id} already exists");

public sealed class InsufficientFundsException(string id, int balance, int amount)
    : Exception($"Account {id} has balance {balance}, cannot withdraw {amount}");

public sealed class InvalidAmountException(int amount)
    : Exception($"Amount must be greater than zero (got {amount})");

public sealed class UnknownEventException(string eventType)
    : Exception($"Unknown event type: {eventType}");

public sealed record StoredEvent(
    long EventNumber,
    string EventId,
    string EventType,
    int Amount,
    string Timestamp);
