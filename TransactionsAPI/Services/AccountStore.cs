public sealed class ConcurrencyConflictException(string id)
    : Exception($"Account {id}: no se pudo appendear tras los reintentos (concurrencia)");

public sealed class AccountStore
{
    private const int MaxRetries = 3;

    private readonly IEventStore _eventStore;
    private readonly ILogger<AccountStore> _logger;

    public AccountStore(IEventStore eventStore, ILogger<AccountStore> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    public static string StreamName(string accountId) => $"account-{accountId}";

    public async Task<(int Balance, long Revision)> CreateAsync(string accountId, CancellationToken ct)
    {
        var pending = Account.NewCreatedEvent(accountId);

        // ExpectedVersion -1 (NoStream): el append solo pasa si el stream no existe.
        var result = await _eventStore.AppendAsync(StreamName(accountId), -1, pending, ct);
        if (result == AppendResult.WrongExpectedVersion)
            throw new AccountAlreadyExistsException(accountId);

        return (0, 0);
    }

    public async Task<(int Balance, long Revision)> DepositAsync(string accountId, int amount, CancellationToken ct) =>
        await ExecuteAsync(accountId, account => account.Deposit(amount), ct);

    public async Task<(int Balance, long Revision)> WithdrawAsync(string accountId, int amount, CancellationToken ct) =>
        await ExecuteAsync(accountId, account => account.Withdraw(amount), ct);

    private async Task<(int Balance, long Revision)> ExecuteAsync(
        string accountId, Func<Account, PendingEvent> command, CancellationToken ct)
    {
        for (var attempt = 0; attempt <= MaxRetries; attempt++)
        {
            var events = await _eventStore.ReadStreamAsync(StreamName(accountId), ct)
                ?? throw new AccountNotFoundException(accountId);

            var account = Account.Fold(accountId, events);

            // Las invariantes se validan AQUI, antes de que el evento exista.
            var pending = command(account);

            var result = await _eventStore.AppendAsync(StreamName(accountId), account.Version, pending, ct);

            if (result == AppendResult.Success)
            {
                var newRevision = account.Version + 1;
                var newBalance = pending.EventType switch
                {
                    AccountEventTypes.MoneyDeposited => account.Balance + pending.Amount,
                    AccountEventTypes.MoneyWithdrawn => account.Balance - pending.Amount,
                    _ => account.Balance
                };
                _logger.LogInformation(
                    "{EventType} appendeado a {Stream} en revision {Revision} (intento {Attempt})",
                    pending.EventType, StreamName(accountId), newRevision, attempt + 1);
                return (newBalance, newRevision);
            }

            // Otro escritor appendeo entre la lectura y el append: volver a cargar
            // el estado real y revalidar el comando contra esa version.
            _logger.LogWarning(
                "WrongExpectedVersion en {Stream} (intento {Attempt}/{Max}), reintentando",
                StreamName(accountId), attempt + 1, MaxRetries + 1);
        }

        throw new ConcurrencyConflictException(accountId);
    }
}
