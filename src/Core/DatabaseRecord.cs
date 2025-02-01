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
        var properties = propertyInfos.Where(prop => prop.GetCustomAttribute<PostgresColumn>()?.Name is not null).ToReadOnlyList();

        var identityProp = propertyInfos.SingleOrDefault(prop => prop.GetCustomAttribute<Identity>() is not null);
        if (identityProp is not null) 
            properties = properties.Where(prop => prop.GetCustomAttribute<Identity>() is null).ToReadOnlyList();

        var propertyNames = string.Join(", ", properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name}"));
        var propertyValues = string.Join(", ", properties.Select(TranslatePropValue));

        var updateColumns = string.Join(
            separator: ", ",
            values: properties.Select(prop => $"{prop.GetCustomAttribute<PostgresColumn>()!.Name} = {TranslatePropValue(prop)}"));

        if (identityProp is not null)
            return UpdateOnIfConditionScript(propertyNames, propertyValues, identityProp, updateColumns);
        
        return InsertOnConflictScript(
            propNames: propertyNames, 
            propValues: propertyValues, 
            primaryKeyColumn: propertyInfos.Single(prop => prop.GetCustomAttribute<PrimaryKey>() is not null).GetCustomAttribute<PostgresColumn>()!.Name, 
            updateColumns: updateColumns);
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
    private string InsertOnConflictScript(string propNames, string propValues, string primaryKeyColumn, string updateColumns)
    {
        return $"""
                INSERT INTO {TableName} ({propNames}) VALUES ({propValues})
                ON CONFLICT ({primaryKeyColumn}) DO UPDATE
                SET {updateColumns};
                """;
    }

    private string UpdateOnIfConditionScript(string propNames, string propValues, PropertyInfo identityProp, string updateColumns)
    {
        return $"""
                DO $$ 
                DECLARE 
                    record_exists BOOLEAN;
                BEGIN
                    SELECT EXISTS(
                        SELECT 1 
                        FROM {TableName} 
                        WHERE {identityProp.GetCustomAttribute<PostgresColumn>()!.Name} = {identityProp.GetValue(this)}
                    ) INTO record_exists;
                
                    IF record_exists THEN
                        UPDATE {TableName} 
                        SET {updateColumns}
                        WHERE {identityProp.GetCustomAttribute<PostgresColumn>()!.Name} = {identityProp.GetValue(this)};
                    ELSE
                        INSERT INTO {TableName} ({propNames}) VALUES ({propValues});
                    END IF;
                END $$;
                """;
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
