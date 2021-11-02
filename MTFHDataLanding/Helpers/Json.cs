using System.Text.Json;
using System.Text.Json.Serialization;

namespace MTFHDataLanding.Helpers
{
    public static class Json
    {
        public static JsonSerializerOptions CreateJsonOptions()
        {
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true
            };
            options.Converters.Add(new JsonStringEnumConverter());
            return options;
        }
    }
}
