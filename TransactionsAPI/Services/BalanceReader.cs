using Microsoft.Data.Sqlite;

public interface IBalanceReader
{
    int? GetBalance(string accountId);
}

public class SqliteBalanceReader : IBalanceReader
{
    private readonly string _connectionString;
    private readonly ILogger<SqliteBalanceReader> _logger;

    public SqliteBalanceReader(IConfiguration config, ILogger<SqliteBalanceReader> logger)
    {
        _logger = logger;
        var path = Environment.GetEnvironmentVariable("DB_PATH")
                   ?? config.GetValue<string>("Store:DbPath")
                   ?? "data.db";
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
        }.ToString();
    }

    public int? GetBalance(string accountId)
    {
        using var conn = new SqliteConnection(_connectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT balance FROM account_balances WHERE account_id = $id";
        cmd.Parameters.AddWithValue("$id", accountId);

        var result = cmd.ExecuteScalar();
        if (result is null or DBNull)
        {
            return null;
        }
        return Convert.ToInt32(result);
    }
}
