using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Shared;

namespace StressTest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string configPath = Path.Combine(AppContext.BaseDirectory, "appsettings.txt");
            string ipAPI = ConfigLoader.Get(configPath, "API_HOST", "127.0.0.2");
            string portAPI = ConfigLoader.Get(configPath, "API_PORT", "5001");

            int count = int.Parse(ConfigLoader.Get(configPath, "COUNT", "500"));
            int delayMs = int.Parse(ConfigLoader.Get(configPath, "DELAY_MS", "20"));

            var client = new HttpClient { BaseAddress = new Uri($"http://{ipAPI}:{portAPI}") };
            var rnd = new Random();

            for (int i = 1; i <= count; i++)
            {
                var payload = new
                {
                    email = $"user{i}@example.com",
                    password = "secret",
                    cardNumber = $"{rnd.Next(1000, 9999)}{rnd.Next(1000, 9999)}{rnd.Next(1000, 9999)}{rnd.Next(1000, 9999)}",
                    items = new[]
                    {
                        new
                        {
                            productId = rnd.Next(1, 11),
                            quantity = (i % 5) + 1
                        }
                    },
                    deliveryType = i % 2 == 0 ? "express" : "standard",
                    address = $"г. Минск, ул. Тестовая, д.{i}"
                };

                var json = JsonSerializer.Serialize(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                try
                {
                    var response = await client.PostAsync("/api/orders", content);
                    if (response.IsSuccessStatusCode)
                    {
                        var responseBody = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"[{i}] OK: {responseBody}");
                    }
                    else
                    {
                        Console.WriteLine($"[{i}] Ошибка {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{i}] Исключение: {ex.Message}");
                }

                if (delayMs > 0)
                    await Task.Delay(delayMs);
            }
        }
    }
}