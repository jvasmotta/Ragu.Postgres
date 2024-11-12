using System.Data;
using System.Reflection;
using System.Text.Json;
using Dapper;

namespace Ragu.Postgres;

public static class TypeMapper
{
    public static void Initialize(string @namespace)
    {
        SqlMapper.AddTypeHandler(new StringArrayTypeHandler());

        var types = from assem in AppDomain.CurrentDomain.GetAssemblies().ToList()
                    from type in assem.GetTypes()
                    where type.IsClass && type.Namespace == @namespace
                    select type;

        types.ToList().ForEach(type =>
        {
            var mapper = (SqlMapper.ITypeMap)Activator
                .CreateInstance(typeof(PostgresColumnTypeMapper<>)
                    .MakeGenericType(type))!;
            SqlMapper.SetTypeMap(type, mapper);

            RegisterJsonbTypeHandlersForNestedTypes(type);
        });
    }

    private static void RegisterJsonbTypeHandlersForNestedTypes(Type type)
    {
        var nestedTypes = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.PropertyType)
            .Where(t => t.IsClass 
                        && t != typeof(string) 
                        && t != typeof(byte[]) 
                        && t != typeof(string[]) 
                        && !t.IsAbstract
            );

        foreach (var nestedType in nestedTypes)
        {
            if (!SqlMapper.HasTypeHandler(nestedType))
            {
                var handlerType = typeof(JsonbTypeHandler<>).MakeGenericType(nestedType);
                var handlerInstance = (SqlMapper.ITypeHandler)Activator.CreateInstance(handlerType)!;
                SqlMapper.AddTypeHandler(nestedType, handlerInstance);

                RegisterJsonbTypeHandlersForNestedTypes(nestedType);
            }
        }
    }
}

public class StringArrayTypeHandler : SqlMapper.TypeHandler<List<string>>
{
    public override void SetValue(IDbDataParameter parameter, List<string>? value) => parameter.Value = value?.ToArray() ?? [];

    public override List<string> Parse(object value) => value is string[] array ? new List<string>(array) : new List<string>();
}

public class JsonbTypeHandler<T> : SqlMapper.TypeHandler<T> where T : class
{
    public override void SetValue(IDbDataParameter parameter, T? value)
    {
        parameter.Value = value == null ? DBNull.Value : JsonSerializer.Serialize(value, JsonSerializerUtils.DefaultOptions);
        parameter.DbType = DbType.String;
    }

    public override T? Parse(object? value)
    {
        return value == null ? default : JsonSerializer.Deserialize<T>(value.ToString() ?? string.Empty);
    }
}

public class FallBackTypeMapper(IEnumerable<SqlMapper.ITypeMap> mappers) : SqlMapper.ITypeMap
{
    public ConstructorInfo? FindConstructor(string[] names, Type[] types)
    {
        foreach (var mapper in mappers)
        {
            try
            {
                var result = mapper.FindConstructor(names, types);
                if (result != null)
                    return result;
            }
            catch (NotImplementedException)
            {
                // the CustomPropertyTypeMap only supports a no-args
                // constructor and throws a not implemented exception.
                // to work around that, catch and ignore.
            }
        }
        return null;
    }

    public ConstructorInfo? FindExplicitConstructor()
    {
        return mappers
            .Select(m => m.FindExplicitConstructor())
            .FirstOrDefault(result => result != null);
    }

    public SqlMapper.IMemberMap? GetConstructorParameter(ConstructorInfo constructor, string columnName)
    {
        foreach (var mapper in mappers)
        {
            try
            {
                var result = mapper.GetConstructorParameter(constructor, columnName);
                if (result != null)
                    return result;
            }
            catch (NotImplementedException)
            {
                // the CustomPropertyTypeMap only supports a no-args
                // constructor and throws a not implemented exception.
                // to work around that, catch and ignore.
            }
        }
        return null;
    }

    public SqlMapper.IMemberMap? GetMember(string columnName)
    {
        foreach (var mapper in mappers)
        {
            try
            {
                var result = mapper.GetMember(columnName);
                if (result != null)
                    return result;
            }
            catch (NotImplementedException)
            {
                // the CustomPropertyTypeMap only supports a no-args
                // constructor and throws a not implemented exception.
                // to work around that, catch and ignore.
            }
        }
        return null;
    }
}

public class PostgresColumnTypeMapper<T>() : FallBackTypeMapper(new SqlMapper.ITypeMap[]
{
    new CustomPropertyTypeMap(typeof(T),
        (type, columnName) =>
            type.GetProperties().FirstOrDefault(prop =>
                prop.GetCustomAttributes(false)
                    .OfType<PostgresColumn>()
                    .Any(attribute => attribute.Name == columnName)
            )!
    ),
    new DefaultTypeMap(typeof(T))
});