using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.AddSingleton<IPublisher, KafkaPublisher>();
builder.Services.AddSingleton<IBalanceReader, SqliteBalanceReader>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

List<string> accounts = new()
{
    "111", "222"
};

app.MapPost("/accounts", async (CreateAccountRequest request, IPublisher publisher) =>
{
    string id = request.AccountId;
    if (id.Length == 0 || string.IsNullOrWhiteSpace(id))
        return Results.BadRequest("AccountId must not be empty and have format YYY");

    bool alreadyExist = accounts.Any(x => x.Equals(request.AccountId));

    if (alreadyExist)
        return Results.BadRequest($"Account {request.AccountId} already exists");

    accounts.Add(id);

    await publisher.Publish("AccountCreated", request.AccountId, null);

    return Results.Created();
})
.WithName("CreateAccount");

app.MapPost("/accounts/{id}/deposit",
    async (string id, DepositRequest request, IPublisher publisher) =>
{
    bool exists = accounts.Any(x => x.Equals(id));
    if (!exists)
        return Results.NotFound($"Account {id} not found");

    var amount = request.Amount;
    if (amount <= 0)
        return Results.BadRequest("Amount must be greater than zero");

    await publisher.Publish("MoneyDeposited", id, amount);

    return Results.Ok();
})
.WithName("DepositAmount");

app.MapPost("/accounts/{id}/withdraw",
    async (string id, WithdrawRequest request, IPublisher publisher) =>
{
    bool exists = accounts.Any(x => x.Equals(id));
    if (!exists)
        return Results.NotFound($"Account {id} not found");

    var amount = request.Amount;
    if (amount <= 0)
        return Results.BadRequest("Amount must be greater than zero");

    await publisher.Publish("MoneyWithdrawn", id, amount);

    return Results.Ok();
})
.WithName("WithdrawAmount");

app.MapGet("/accounts/{id}/balance", (string id, IBalanceReader reader) =>
{
    var balance = reader.GetBalance(id);
    return balance is null
        ? Results.NotFound($"Account {id} not found")
        : Results.Ok(new { accountId = id, balance });
})
.WithName("GetBalance");

app.Run();

record CreateAccountRequest
{
    public required string AccountId { get; set; } = "";
}

public record DepositRequest(int Amount);
public record WithdrawRequest(int Amount);
