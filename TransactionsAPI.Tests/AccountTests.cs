using Xunit;

namespace TransactionsAPI.Tests;

public class AccountTests
{
    private static StoredEvent Ev(long number, string type, int amount) =>
        new(number, Guid.NewGuid().ToString(), type, amount, "2026-10-01T00:00:00Z");

    [Fact]
    public void FoldCalculaBalanceYRevision()
    {
        var events = new List<StoredEvent>
        {
            Ev(0, AccountEventTypes.AccountCreated, 0),
            Ev(1, AccountEventTypes.MoneyDeposited, 100),
            Ev(2, AccountEventTypes.MoneyWithdrawn, 40),
        };

        var account = Account.Fold("901", events);

        Assert.Equal(60, account.Balance);
        Assert.Equal(2, account.Version);
        Assert.True(account.Exists);
    }

    [Fact]
    public void CuentaSinStreamNoExiste()
    {
        var account = Account.Fold("901", new List<StoredEvent>());

        Assert.False(account.Exists);
        Assert.Equal(-1, account.Version);
        Assert.Throws<AccountNotFoundException>(() => account.Deposit(10));
    }

    [Fact]
    public void WithdrawSinSaldoLanzaInsufficientFunds()
    {
        var account = Account.Fold("901", new List<StoredEvent>
        {
            Ev(0, AccountEventTypes.AccountCreated, 0),
            Ev(1, AccountEventTypes.MoneyDeposited, 100),
        });

        var ex = Assert.Throws<InsufficientFundsException>(() => account.Withdraw(101));
        Assert.Contains("cannot withdraw", ex.Message);
    }

    [Fact]
    public void MontosNoPositivosSeRechazan()
    {
        var account = Account.Fold("901", new List<StoredEvent>
        {
            Ev(0, AccountEventTypes.AccountCreated, 0),
        });

        Assert.Throws<InvalidAmountException>(() => account.Withdraw(0));
        Assert.Throws<InvalidAmountException>(() => account.Deposit(-5));
    }

    [Fact]
    public void WithdrawValidoDevuelveEventoConIdentidad()
    {
        var account = Account.Fold("901", new List<StoredEvent>
        {
            Ev(0, AccountEventTypes.AccountCreated, 0),
            Ev(1, AccountEventTypes.MoneyDeposited, 100),
        });

        var pending = account.Withdraw(30);

        Assert.Equal(AccountEventTypes.MoneyWithdrawn, pending.EventType);
        Assert.Equal(30, pending.Amount);
        Assert.Equal("901", pending.AccountId);
        Assert.False(string.IsNullOrEmpty(pending.EventId));
    }

    [Fact]
    public void FoldAnteEventoDesconocidoLanza()
    {
        Assert.Throws<UnknownEventException>(() => Account.Fold("901", new List<StoredEvent>
        {
            Ev(0, "SomethingWeird", 0),
        }));
    }

    [Fact]
    public async Task DobleGastoContraLaMismaFotoSoloPuedeAppendearUnaVez()
    {
        // El escenario de la limitacion 1 de la V1: dos retiros de 100 contra
        // un saldo de 100. Cada agregado valida contra SU foto del estado,
        // asi que ambos producen un evento "valido" localmente. La garantia
        // la pone el append condicional: el primero deja el stream en la
        // revision 2 y el segundo, que esperaba la revision 1, es rechazado
        // (WrongExpectedVersion) y su evento nunca nace.
        var events = new List<StoredEvent>
        {
            Ev(0, AccountEventTypes.AccountCreated, 0),
            Ev(1, AccountEventTypes.MoneyDeposited, 100),
        };

        var first = Account.Fold("901", events);
        var second = Account.Fold("901", events);
        var pendingFirst = first.Withdraw(100);
        var pendingSecond = second.Withdraw(100);

        var es = new FakeEventStore { Exists = true };
        es.Events.Add(Ev(0, AccountEventTypes.AccountCreated, 0));
        es.Events.Add(Ev(1, AccountEventTypes.MoneyDeposited, 100));

        var r1 = await es.AppendAsync("account-901", 1, pendingFirst);
        var r2 = await es.AppendAsync("account-901", 1, pendingSecond);

        Assert.Equal(AppendResult.Success, r1);
        Assert.Equal(AppendResult.WrongExpectedVersion, r2);
        Assert.Equal(1, es.Events.Count(e => e.EventType == AccountEventTypes.MoneyWithdrawn));
    }
}
