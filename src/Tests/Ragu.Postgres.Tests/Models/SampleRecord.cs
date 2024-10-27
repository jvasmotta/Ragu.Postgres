using System.Reflection;

namespace Ragu.Postgres.Tests.Models;

public enum SampleEnum { Option1, Option2 }

public record SampleRecord(
    [property: PrimaryKey, PostgresColumn("id")] string Id,
    [property: PostgresColumn("string_column")] string StringProp,
    [property: PostgresColumn("date_column")] DateTime DateTimeProp,
    [property: PostgresColumn("bool_column")] bool BoolProp,
    [property: PostgresColumn("bytes_column")] byte[] ByteArrayProp,
    [property: PostgresColumn("null_column")] object? NullProp,
    [property: PostgresColumn("array_column")] string[] StringArrayProp,
    [property: PostgresColumn("enum_column")] SampleEnum EnumProp,
    [property: PostgresColumn("list_column")] List<string> StringListProp,
    [property: PostgresColumn("int_column")] int IntProp) : DatabaseRecord("sample_records")
{
    public SampleRecord() : this(
        Id: null!,
        StringProp: null!,
        DateTimeProp: DateTime.MinValue,
        BoolProp: false,
        ByteArrayProp: [],
        NullProp: null,
        StringArrayProp: [],
        EnumProp: SampleEnum.Option1,
        StringListProp: [],
        IntProp: 0)
    { }

    public static SampleRecord CreateSample(string id) => new(
        Id: id,
        StringProp: "TestString",
        DateTimeProp: DateTime.MinValue,
        BoolProp: true,
        ByteArrayProp: [0x1, 0x2, 0x3],
        NullProp: null,
        StringArrayProp: ["item1", "item2"],
        EnumProp: SampleEnum.Option1,
        StringListProp: new List<string> { "listItem1", "listItem2" },
        IntProp: 123);

    protected override PropertyInfo[] GetProperties() => typeof(SampleRecord).GetProperties();

    public static BasicPostgresHandler<SampleRecord>.TableName GetTableName() => new("sample_records");

    public static BasicPostgresHandler<SampleRecord>.Query GetFromId(string id) => new(
        Value: "SELECT * FROM sample_records sr WHERE sr.id = @id",
        Parameters: new { id });
}