using Dapper;
using Npgsql;
using DiscriminatedOnions;

namespace Ragu.Postgres;

public static class BasicPostgresHandler
{
    private static string? _connectionString;

    public static void SetConnectionString(string connectionString) => _connectionString = connectionString;

    internal static string? GetConnectionString() => _connectionString;

    internal static void EnsureConnectionStringIsSet()
    {
        if (string.IsNullOrEmpty(_connectionString))
            throw new InvalidOperationException("Connection string is not set. Call 'SetConnectionString' before executing any operations.");
    }
}

public static class BasicPostgresHandler<T>
{
    public record struct Query(string Value, object? Parameters);
    public record struct TableName(string Value);

    public static Option<T> Get(Query query)
    {
        BasicPostgresHandler.EnsureConnectionStringIsSet();
        using var connection = new NpgsqlConnection(BasicPostgresHandler.GetConnectionString());
        connection.Open();
        var result = connection.QueryFirstOrDefault<T?>(query.Value, query.Parameters);
        return result is null
            ? Option.None<T>()
            : Option.Some(result);
    }

    public static IEnumerable<T> Enumerate(Query query)
    {
        BasicPostgresHandler.EnsureConnectionStringIsSet();
        using var connection = new NpgsqlConnection(BasicPostgresHandler.GetConnectionString());
        connection.Open();
        return connection.Query<T>(query.Value, query.Parameters);
    }

    public static IEnumerable<T> Enumerate(TableName tableName)
    {
        BasicPostgresHandler.EnsureConnectionStringIsSet();
        using var connection = new NpgsqlConnection(BasicPostgresHandler.GetConnectionString());
        connection.Open();
        return connection.Query<T>($"SELECT * FROM {tableName.Value}");
    }

    public static int Upsert(DatabaseRecord record)
    {
        BasicPostgresHandler.EnsureConnectionStringIsSet();
        using var connection = new NpgsqlConnection(BasicPostgresHandler.GetConnectionString());
        connection.Open();
        return connection.Execute(record.GetUpsertQuery());
    }

    public static int Insert(DatabaseRecord record) => Insert([record]);

    public static int Insert(IEnumerable<DatabaseRecord> records)
    {
        BasicPostgresHandler.EnsureConnectionStringIsSet();
        using var connection = new NpgsqlConnection(BasicPostgresHandler.GetConnectionString());
        connection.Open();
        foreach (var databaseRecord in records)
            connection.Execute(databaseRecord.GetInsertQuery());

        return 1;
    }
}