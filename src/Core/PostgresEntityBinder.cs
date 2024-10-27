using System.Data;
using System.Reflection;
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
        });
    }
}

public class StringArrayTypeHandler : SqlMapper.TypeHandler<List<string>>
{
    public override void SetValue(IDbDataParameter parameter, List<string>? value) => parameter.Value = value?.ToArray() ?? [];

    public override List<string> Parse(object value) => value is string[] array ? new List<string>(array) : new List<string>();
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