using System.Text.Json;
using System.Text.Json.Serialization;

namespace Ragu.Postgres;

public static class JsonSerializerUtils
{
    public static readonly JsonSerializerOptions DefaultOptions = new() { Converters = { new JsonStringEnumConverter() }, PropertyNameCaseInsensitive = true };
}