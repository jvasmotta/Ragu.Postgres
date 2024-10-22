using System.Reflection;
using DiscriminatedOnions;

namespace Ragu.Postgres;

public abstract record DatabaseRecord(string TableName)
{
    protected abstract PropertyInfo[] GetProperties();

    public string GetUpsertQuery()
    {
        var properties = GetProperties().Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null).ToReadOnlyList();
        var propertyNames = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name}"));
        var propertyValues = string.Join(", ", properties.Select(TranslatePropValue));
        var updateColumns = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name} = {TranslatePropValue(prop)}"));
        return $"""
                INSERT INTO {TableName} ({propertyNames}) VALUES ({propertyValues})
                ON CONFLICT ({GetProperties().Single(prop => prop.GetCustomAttribute<PrimaryKey>() is not null).GetCustomAttribute<PostgresColumn>()!.Name}) DO UPDATE
                SET {updateColumns};
                """;
    }

    public string GetInsertQuery()
    {
        var properties = GetProperties().Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null).ToReadOnlyList();
        var propertyNames = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name}"));
        var propertyValues = string.Join(", ", properties.Select(TranslatePropValue));

        return $"INSERT INTO {TableName} ({propertyNames}) VALUES ({propertyValues})";
    }

    private string TranslatePropValue(PropertyInfo prop)
    {
        var value = prop.GetValue(this);
        return value switch
        {
            string => $"'{value}'",
            DateTime => $"'{value:O}'",
            bool b => $"{(b ? "true" : "false")}",
            byte[] bytes => $@"E'\\x{BitConverter.ToString(bytes).Replace("-", "").ToLower()}'",
            null => "null",
            _ when prop.PropertyType.IsEnum => $"'{value}'",
            _ => $"{value}"
        };
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class PrimaryKey : Attribute;

[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public class PostgresColumn(string name) : Attribute
{
    public string Name { get; } = name;
}