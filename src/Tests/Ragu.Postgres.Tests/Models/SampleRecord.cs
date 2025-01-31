using System.Reflection;

namespace Ragu.Postgres.Tests.Models;

public enum SampleEnum { Option1, Option2 }

public record SampleRecord(
    [property: Identity, PrimaryKey, PostgresColumn("id")] long Id,
    [property: PostgresColumn("string_column")] string StringProp,
    [property: PostgresColumn("date_column")] DateTime DateTimeProp,
    [property: PostgresColumn("bool_column")] bool BoolProp,
    [property: PostgresColumn("bytes_column")] byte[] ByteArrayProp,
    [property: PostgresColumn("null_column")] object? NullProp,
    [property: PostgresColumn("array_column")] string[] StringArrayProp,
    [property: PostgresColumn("enum_column")] SampleEnum EnumProp,
    [property: PostgresColumn("list_column")] List<string> StringListProp,
    [property: PostgresColumn("int_column")] int IntProp,
    [property: PostgresColumn("nested_column")] SampleRecord.NestedObject? NestedObjectProp) : DatabaseRecord("sample_records")
{
    public record NestedObject(string NestedString, int NestedInteger, DateTime NestedDateTime);
    
    public SampleRecord() : this(
        Id: 0,
        StringProp: null!,
        DateTimeProp: DateTime.MinValue,
        BoolProp: false,
        ByteArrayProp: [],
        NullProp: null,
        StringArrayProp: [],
        EnumProp: SampleEnum.Option1,
        StringListProp: [],
        IntProp: 0,
        NestedObjectProp: null)
    { }

    public static SampleRecord CreateSample() => new(
        Id: 0,
        StringProp: "TestString",
        DateTimeProp: DateTime.MinValue,
        BoolProp: true,
        ByteArrayProp: [0x1, 0x2, 0x3],
        NullProp: null,
        StringArrayProp: ["item1", "item2"],
        EnumProp: SampleEnum.Option1,
        StringListProp: new List<string> { "listItem1", "listItem2" },
        IntProp: 123,
        NestedObjectProp: new NestedObject("NestedString", 456, DateTime.MinValue));

    protected override PropertyInfo[] GetProperties() => typeof(SampleRecord).GetProperties();

    public static BasicPostgresHandler<SampleRecord>.TableName GetTableName() => new("sample_records");

    public static BasicPostgresHandler<SampleRecord>.Query GetFromId(long id) => new(
        Value: "SELECT * FROM sample_records sr WHERE sr.id = @id",
        Parameters: new { id });
}