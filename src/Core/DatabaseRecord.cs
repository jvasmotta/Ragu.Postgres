using System.Reflection;
using System.Text.Json;
using DiscriminatedOnions;

namespace Ragu.Postgres;

public abstract record DatabaseRecord(string TableName)
{
    protected abstract PropertyInfo[] GetProperties();

    public string GetUpsertQuery()
    {
        var propertyInfos = GetProperties();
        var identityProp = propertyInfos.SingleOrDefault(prop => prop.GetCustomAttribute<Identity>() is not null);
        if (identityProp is null)
        {
            var properties = propertyInfos
                .Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null)
                .ToReadOnlyList();

            var propertyNames = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name}"));
            var propertyValues = string.Join(", ", properties.Select(TranslatePropValue));

            var primaryKeyColumn = propertyInfos
                .Single(prop => prop.GetCustomAttribute<PrimaryKey>() is not null)
                .GetCustomAttribute<PostgresColumn>()!.Name;

            var updateColumns = string.Join(", ", properties
                .Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name} = {TranslatePropValue(prop)}"));

            return $"""
                    INSERT INTO {TableName} ({propertyNames}) VALUES ({propertyValues})
                    ON CONFLICT ({primaryKeyColumn}) DO UPDATE
                    SET {updateColumns};
                    """;
        }
        else
        {
            var properties = propertyInfos
                .Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null)
                .Where(prop => prop.GetCustomAttribute<Identity>() is null)
                .ToReadOnlyList();

            var propertyNames = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name}"));
            var propertyValues = string.Join(", ", properties.Select(TranslatePropValue));

            var updateColumns = string.Join(
                separator: ", ", 
                values: properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name} = {TranslatePropValue(prop)}")
            );
            
            var whereCondition = $"WHERE {identityProp.GetCustomAttribute<PostgresColumn>()!.Name} = {identityProp.GetValue(this)}";
            return $"""
                    DO $$ 
                    DECLARE 
                        record_exists BOOLEAN;
                    BEGIN
                        SELECT EXISTS(
                            SELECT 1 
                            FROM {TableName} 
                            {whereCondition}
                        ) INTO record_exists;
                    
                        IF record_exists THEN
                            UPDATE {TableName} 
                            SET {updateColumns}
                            {whereCondition};
                        ELSE
                            INSERT INTO {TableName} ({propertyNames}) VALUES ({propertyValues});
                        END IF;
                    END $$;
                    """;
        }
    }

    public string GetInsertQuery()
    {
        var properties = GetProperties()
            .Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null)
            .Where(prop => prop.GetCustomAttribute<Identity>() is null)
            .ToReadOnlyList();

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
            Array array => $"'{{{string.Join(", ", array.Cast<object>().Select(v => $"\"{v}\""))}}}'",
            null => "null",
            _ when typeof(IEnumerable<string>).IsAssignableFrom(prop.PropertyType) => $"'{{{string.Join(", ", ((IEnumerable<string>)value!).Select(v => $"\"{v}\""))}}}'",
            _ when prop.PropertyType.IsEnum => $"'{value}'",
            _ when prop.PropertyType.IsClass && prop.PropertyType != typeof(string) => $"'{JsonSerializer.Serialize(value, JsonSerializerUtils.DefaultOptions).Replace("'", "''")}'",
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

[AttributeUsage(AttributeTargets.Property)]
public class Identity : Attribute;
