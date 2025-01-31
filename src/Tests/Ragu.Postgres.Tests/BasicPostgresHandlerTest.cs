using System.Text.Json;
using Dapper;
using FluentAssertions;
using Npgsql;
using Ragu.Postgres.Tests.Models;

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
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sample_records') THEN
        CREATE TABLE IF NOT EXISTS sample_records (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                string_column TEXT,
                date_column TIMESTAMP,
                bool_column BOOLEAN,
                bytes_column BYTEA,
                null_column TEXT,
                array_column TEXT[],
                enum_column TEXT,
                list_column TEXT[],
                int_column INTEGER,
                nested_column JSONB
            );
    END IF;
END $$;", testDbConnection).ExecuteNonQuery();

        BasicPostgresHandler.SetConnectionString(ConnectionString);
        TypeMapper.Initialize("Ragu.Postgres.Tests.Models");
    }

    [OneTimeTearDown]
    public void TearDown()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("DROP TABLE sample_records;", connection);
        cmd.ExecuteNonQuery();
    }

    [Test, Order(1)]
    public void Insert()
    {
        // Arrange
        BasicPostgresHandler<SampleRecord>.Insert(SampleRecord.CreateSample());

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();

        reader["id"].Should().BeEquivalentTo(1);
        reader["string_column"].Should().BeEquivalentTo("TestString");
        reader["date_column"].Should().BeEquivalentTo(DateTime.MinValue);
        reader["bool_column"].Should().BeEquivalentTo(true);
        reader["bytes_column"].Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
        reader["null_column"].Should().BeEquivalentTo(DBNull.Value);
        reader["array_column"].Should().BeEquivalentTo(new[] { "item1", "item2" });
        reader["enum_column"].Should().BeEquivalentTo("Option1");
        reader["list_column"].Should().BeEquivalentTo(new[] { "listItem1", "listItem2" });
        reader["int_column"].Should().BeEquivalentTo(123);

        JsonSerializer.Deserialize<SampleRecord.NestedObject>(reader["nested_column"].ToString() ?? string.Empty)
            .Should()
            .BeEquivalentTo(new SampleRecord.NestedObject("NestedString", 456, DateTime.MinValue));
    }

    [Test, Order(2)]
    public void Get()
    {
        // Arrange
        var actual = BasicPostgresHandler<SampleRecord>.Get(SampleRecord.GetFromId(1));

        // Assert
        actual.IsSome.Should().BeTrue();
        actual.Value.Should().BeEquivalentTo(SampleRecord.CreateSample() with { Id = 1 });
    }

    [Test, Order(3)]
    public void Enumerate()
    {
        BasicPostgresHandler<SampleRecord>.Insert(SampleRecord.CreateSample());
        var records = BasicPostgresHandler<SampleRecord>.Enumerate(SampleRecord.GetTableName()).ToList();
        records.Should().HaveCount(2);
        records.Should().BeEquivalentTo(new List<SampleRecord>
        {
            SampleRecord.CreateSample() with { Id = 1 },
            SampleRecord.CreateSample() with { Id = 2 }
        });
    }

    [Test, Order(4)]
    public void Replace()
    {
        // Arrange
        var modifiedSample = SampleRecord.CreateSample() with
        {
            Id = 1,
            IntProp = 444,
            StringListProp = ["Modified List"]
        };
        BasicPostgresHandler<SampleRecord>.Upsert(modifiedSample);

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records sr WHERE sr.id = 1 LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();

        reader["id"].Should().BeEquivalentTo(1);
        reader["list_column"].Should().BeEquivalentTo(new[] { "Modified List" });
        reader["int_column"].Should().BeEquivalentTo(444);
    }

    [Test, Order(5)]
    public void DeleteOne()
    {
        //Arrange
        BasicPostgresHandler<SampleRecord>.Delete(SampleRecord.GetTableName(), "id = 2");

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records sr WHERE sr.id = 2 LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeFalse();
    }

    [Test, Order(6)]
    public void Truncate()
    {
        //Arrange
        BasicPostgresHandler<SampleRecord>.Truncate(SampleRecord.GetTableName());

        //Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records", connection);
        using var reader = cmd.ExecuteReader();
        reader.HasRows.Should().BeFalse();
    }
}