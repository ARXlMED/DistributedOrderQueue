using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace StressTest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            int count = args.Length > 0 ? int.Parse(args[0]) : 500;
            int delayMs = args.Length > 1 ? int.Parse(args[1]) : 1000;

            var client = new HttpClient { BaseAddress = new Uri("http://127.0.0.2:5001") };
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
                            productId = $"p-{i}",
                            name = $"Товар {i}",
                            quantity = (i % 5) + 1,
                            price = (decimal)(9.99 + i)
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