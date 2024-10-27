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
        BasicPostgresHandler<SampleRecord>.Insert(SampleRecord.CreateSample("SAMPLE_ID_1"));

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();

        reader["id"].Should().BeEquivalentTo("SAMPLE_ID_1");
        reader["string_column"].Should().BeEquivalentTo("TestString");
        reader["date_column"].Should().BeEquivalentTo(DateTime.MinValue);
        reader["bool_column"].Should().BeEquivalentTo(true);
        reader["bytes_column"].Should().BeEquivalentTo(new byte[] { 1, 2, 3 });
        reader["null_column"].Should().BeEquivalentTo(DBNull.Value);
        reader["array_column"].Should().BeEquivalentTo(new[] { "item1", "item2" });
        reader["enum_column"].Should().BeEquivalentTo("Option1");
        reader["list_column"].Should().BeEquivalentTo(new[] { "listItem1", "listItem2" });
        reader["int_column"].Should().BeEquivalentTo(123);
    }

    [Test, Order(2)]
    public void Get()
    {
        // Arrange
        var actual = BasicPostgresHandler<SampleRecord>.Get(SampleRecord.GetFromId("SAMPLE_ID_1"));
        
        // Assert
        actual.IsSome.Should().BeTrue();
        actual.Value.Should().BeEquivalentTo(SampleRecord.CreateSample("SAMPLE_ID_1"));
    }

    [Test, Order(3)]
    public void Enumerate()
    {
        BasicPostgresHandler<SampleRecord>.Insert(SampleRecord.CreateSample("SAMPLE_ID_2"));
        var records = BasicPostgresHandler<SampleRecord>.Enumerate(SampleRecord.GetTableName()).ToList();
        records.Should().HaveCount(2);
        records.Should().BeEquivalentTo(new List<SampleRecord>
        {
            SampleRecord.CreateSample("SAMPLE_ID_1"),
            SampleRecord.CreateSample("SAMPLE_ID_2")
        });
    }

    [Test, Order(4)]
    public void Replace()
    {
        // Arrange
        var modifiedSample = SampleRecord.CreateSample("SAMPLE_ID_1") with
        {
            IntProp = 444,
            StringListProp = ["Modified List"]
        };
        BasicPostgresHandler<SampleRecord>.Upsert(modifiedSample);

        // Assert
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM sample_records sr WHERE sr.id = 'SAMPLE_ID_1' LIMIT 1;", connection);
        using var reader = cmd.ExecuteReader();

        reader.Read().Should().BeTrue();

        reader["id"].Should().BeEquivalentTo("SAMPLE_ID_1");
        reader["list_column"].Should().BeEquivalentTo(new[] { "Modified List" });
        reader["int_column"].Should().BeEquivalentTo(444);
    }
}