using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.Configure<EventStoreOptions>(
    builder.Configuration.GetSection(EventStoreOptions.SectionName));
builder.Services.AddHttpClient<EventStoreHttp>();
builder.Services.AddTransient<IEventStore>(sp =>
    sp.GetRequiredService<EventStoreHttp>());
builder.Services.AddTransient<AccountStore>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Una cuenta existe si su stream existe en EventStoreDB: no hay registro en
// memoria y los restarts no pierden nada (limitacion 2a de la V1).
var accountIdPattern = new Regex("^[A-Za-z0-9_-]+$", RegexOptions.Compiled);

app.MapPost("/accounts", async (CreateAccountRequest request, AccountStore store) =>
{
    string id = request.AccountId?.Trim() ?? "";
    if (!accountIdPattern.IsMatch(id))
        return Results.BadRequest("AccountId must be non-empty and use only letters, digits, '_' or '-'");

    try
    {
        await store.CreateAsync(id, CancellationToken.None);
    }
    catch (AccountAlreadyExistsException ex)
    {
        return Results.Conflict(ex.Message);
    }

    return Results.Created();
})
.WithName("CreateAccount");

app.MapPost("/accounts/{id}/deposit",
    async (string id, DepositRequest request, AccountStore store) =>
{
    if (!accountIdPattern.IsMatch(id))
        return Results.BadRequest("Invalid account id");

    return await ExecuteAsync(
        store.DepositAsync(id, request.Amount, CancellationToken.None), id);
})
.WithName("DepositAmount");

app.MapPost("/accounts/{id}/withdraw",
    async (string id, WithdrawRequest request, AccountStore store) =>
{
    if (!accountIdPattern.IsMatch(id))
        return Results.BadRequest("Invalid account id");

    return await ExecuteAsync(
        store.WithdrawAsync(id, request.Amount, CancellationToken.None), id);
})
.WithName("WithdrawAmount");

app.Run();

static async Task<IResult> ExecuteAsync(Task<(int Balance, long Revision)> command, string id)
{
    try
    {
        var (balance, revision) = await command;
        return Results.Ok(new { accountId = id, balance, revision });
    }
    catch (AccountNotFoundException ex)
    {
        return Results.NotFound(ex.Message);
    }
    catch (InsufficientFundsException ex)
    {
        // El retiro sin saldo se rechaza ANTES de que exista el evento:
        // respuesta honesta (409) y el read model nunca diverge (limitacion 1 y 2c).
        return Results.Conflict(ex.Message);
    }
    catch (InvalidAmountException ex)
    {
        return Results.BadRequest(ex.Message);
    }
    catch (ConcurrencyConflictException ex)
    {
        return Results.Conflict(ex.Message);
    }
}

record CreateAccountRequest
{
    public required string AccountId { get; set; } = "";
}

public record DepositRequest(int Amount);
public record WithdrawRequest(int Amount);
