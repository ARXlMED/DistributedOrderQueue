using System.IO;

namespace Shared
{
    public static class ConfigLoader
    {
        public static string Get(string filePath, string key, string defaultValue)
        {
            if (!File.Exists(filePath))
                return defaultValue;

            foreach (var line in File.ReadLines(filePath))
            {
                if (line.StartsWith(key + "="))
                    return line.Substring(key.Length + 1);
            }
            return defaultValue;
        }
    }
}