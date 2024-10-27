using Dapper;
using Npgsql;

namespace Ragu.Postgres.Tests;

[TestFixture]
public class BasicPostgresHandlerTests
{
    private const string ConnectionString = "Host=localhost;Port=5432;Username=user;Password=secret;Database=testdb";

    [OneTimeSetUp]
    public void Setup()
    {
        using var connection = new NpgsqlConnection(string.Join(";", ConnectionString.Split(";").Where(pair => !pair.Trim().StartsWith("Database"))));
        connection.Open();
        if (!connection.ExecuteScalar<bool>("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = 'testdb');"))
            connection.Execute("CREATE DATABASE testdb;");
        connection.Close();

        using var testDbConnection = new NpgsqlConnection(ConnectionString);
        testDbConnection.Open();

        new NpgsqlCommand(@"
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sample_record') THEN
        CREATE TABLE IF NOT EXISTS sample_record (
                id VARCHAR(50) PRIMARY KEY,
                string_column TEXT,
                date_column TIMESTAMP,
                bool_column BOOLEAN,
                bytes_column BYTEA,
                null_column TEXT,
                array_column TEXT[],
                enum_column TEXT,
                list_column TEXT[],
                int_column INTEGER
            );
    END IF;
END $$;", testDbConnection).ExecuteNonQuery();

        BasicPostgresHandler.SetConnectionString(ConnectionString);
    }

    [OneTimeTearDown]
    public void TearDown()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("DROP TABLE sample_record;", connection);
        cmd.ExecuteNonQuery();
    }

    [Test, Order(1)]
    public void Insert()
    {
        // Arrange
        BasicPostgresHandler<SampleRecord>.Insert(new SampleRecord("SAMPLE_ID_1"));

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_record LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        // Ensure at least one row was returned
        Assert.That(reader.Read(), Is.True);

        // Now verify each column's value
        Assert.That(reader["id"], Is.EqualTo("SAMPLE_ID_1"));
        Assert.That(reader["string_column"], Is.EqualTo("TestString"));
        Assert.That(reader["date_column"], Is.EqualTo(DateTime.MinValue));
        Assert.That(reader["bool_column"], Is.EqualTo(true));
        Assert.That(reader["bytes_column"], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(reader["null_column"], Is.EqualTo(DBNull.Value));
        Assert.That(reader["array_column"], Is.EqualTo(new[] { "item1", "item2" }));
        Assert.That(reader["enum_column"], Is.EqualTo("Option1"));
        Assert.That(reader["list_column"], Is.EqualTo(new[] { "listItem1", "listItem2" }));
        Assert.That(reader["int_column"], Is.EqualTo(123));
    }

    [Test, Order(2)]
    public void Get()
    {
        var actual = BasicPostgresHandler<SampleRecord>.Get(SampleRecord.GetFromId("SAMPLE_ID_1"));
        Assert.That(actual.IsSome, Is.EqualTo(true));
        Assert.That(actual.Value, Is.EqualTo(new SampleRecord("SAMPLE_ID_1")));
    }
}