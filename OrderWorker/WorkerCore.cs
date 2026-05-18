using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;

namespace OrderWorker
{
    public class WorkerCore
    {
        private readonly IPAddress brokerIP;
        private readonly int brokerPort;
        private readonly string connectionString;
        private readonly string workerId;

        private readonly IPAddress cardServiceIP;
        private readonly int cardServicePort;

        public WorkerCore(string[] args)
        {
            brokerIP = IPAddress.Parse(
                Environment.GetEnvironmentVariable("BROKER_HOST") ?? "127.0.0.3");
            brokerPort = int.Parse(
                Environment.GetEnvironmentVariable("BROKER_PORT") ?? "5002");
            connectionString = Environment.GetEnvironmentVariable("PG_CONNECTION")
                ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=stalker";

            cardServiceIP = IPAddress.Parse(
                Environment.GetEnvironmentVariable("CARD_SERVICE_HOST") ?? "127.0.0.4");
            cardServicePort = int.Parse(
                Environment.GetEnvironmentVariable("CARD_SERVICE_PORT") ?? "6000");

            workerId = Guid.NewGuid().ToString("N");
        }

        public async Task StartWorking()
        {
            Console.WriteLine($"Воркер {workerId} запущен");
            Console.WriteLine("=--------------------------------------------=");
            Console.WriteLine($"Подключен к брокеру: {brokerIP}:{brokerPort}");
            Console.WriteLine($"Подключен к CardService: {cardServiceIP}:{cardServicePort}");

            while (true)
            {
                try
                {
                    await ProcessNextMessage();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка в главном цикле: {ex.Message}");
                    await Task.Delay(2000);
                }
            }
        }

        private async Task ProcessNextMessage()
        {
            using var consumeSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await consumeSocket.ConnectAsync(new IPEndPoint(brokerIP, brokerPort));

            byte[] consumeCmd = Encoding.UTF8.GetBytes($"CONSUME orders.new {workerId} 30\n");
            await consumeSocket.SendAsync(consumeCmd, SocketFlags.None);

            string? responseLine = await ReceiveLineAsync(consumeSocket);
            if (responseLine == null) return;

            if (responseLine.StartsWith("MESSAGE"))
            {
                string[] parts = responseLine.Split(' ', 2);
                string messageId = parts.Length > 1 ? parts[1] : null;

                string? body = await ReceiveLineAsync(consumeSocket);
                if (body == null || string.IsNullOrWhiteSpace(body))
                {
                    Console.WriteLine("Получено пустое тело сообщения");
                    return;
                }

                var order = JsonSerializer.Deserialize<OrderMessage>(body);
                if (order == null)
                {
                    Console.WriteLine("Не удалось десериализовать заказ");
                    return;
                }

                try { consumeSocket.Shutdown(SocketShutdown.Both); } catch { }
                consumeSocket.Close();

                string? currentStatus = await GetOrderStatus(order.OrderId);
                if (currentStatus == "processed" || currentStatus == "cancelled" || currentStatus == "delivered")
                {
                    Console.WriteLine($"Заказ {order.OrderId} уже имеет статус '{currentStatus}', пропускаем обработку");
                    await DeleteMessageAsync(messageId, order.OrderId);
                    return;
                }

                Console.WriteLine($"Получен заказ {order.OrderId}, сумма {order.TotalAmount}. Обработка...");

                bool paymentSuccess = await TryChargeCard(order.UserId, order.TotalAmount, order.CardNumber);
                if (paymentSuccess)
                {
                    Console.WriteLine($"Оплата заказа {order.OrderId} прошла успешно");
                    await UpdateOrderStatus(order.OrderId, "processed");
                }
                else
                {
                    Console.WriteLine($"Недостаточно средств для заказа {order.OrderId}. Отмена.");
                    await UpdateOrderStatus(order.OrderId, "cancelled");
                }

                await DeleteMessageAsync(messageId, order.OrderId);
            }
            else if (responseLine.StartsWith("NONE"))
            {
                await Task.Delay(2000);
            }
            else
            {
                Console.WriteLine($"Неизвестный ответ брокера: {responseLine}");
            }
        }

        private async Task DeleteMessageAsync(string messageId, int orderId)
        {
            using var deleteSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await deleteSocket.ConnectAsync(new IPEndPoint(brokerIP, brokerPort));
            byte[] deleteCmd = Encoding.UTF8.GetBytes($"DELETE orders.new {messageId} {workerId}\n");
            await deleteSocket.SendAsync(deleteCmd, SocketFlags.None);
            string? deleteResponse = await ReceiveLineAsync(deleteSocket);
            if (deleteResponse != null && deleteResponse.StartsWith("OK"))
            {
                Console.WriteLine($"Сообщение {messageId} удалено из очереди");
            }
            else
            {
                Console.WriteLine($"Не удалось удалить сообщение {messageId}: {deleteResponse}");
            }
        }

        private async Task<bool> TryChargeCard(int userId, decimal amount, string cardNumber)
        {
            try
            {
                using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(new IPEndPoint(cardServiceIP, cardServicePort));

                string cmd = $"CHARGE {userId} {amount} {cardNumber}\n";
                byte[] cmdBytes = Encoding.UTF8.GetBytes(cmd);
                await socket.SendAsync(cmdBytes, SocketFlags.None);

                string? response = await ReceiveLineAsync(socket);
                return response != null && response.StartsWith("OK");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка соединения с CardService: {ex.Message}");
                return false;
            }
        }

        private async Task<string?> GetOrderStatus(int orderId)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT status FROM orders WHERE order_id = @orderId", conn);
            cmd.Parameters.AddWithValue("orderId", orderId);
            var result = await cmd.ExecuteScalarAsync();
            return result?.ToString();
        }

        private async Task UpdateOrderStatus(int orderId, string newStatus)
        {
            try
            {
                await using var conn = new NpgsqlConnection(connectionString);
                await conn.OpenAsync();
                await using var cmd = new NpgsqlCommand(
                    "UPDATE orders SET status = @newStatus, updated_at = @updatedAt WHERE order_id = @orderId",
                    conn);
                cmd.Parameters.AddWithValue("newStatus", newStatus);
                cmd.Parameters.AddWithValue("updatedAt", DateTime.UtcNow);
                cmd.Parameters.AddWithValue("orderId", orderId);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка обновления статуса заказа {orderId}: {ex.Message}");
            }
        }

        private static async Task<string?> ReceiveLineAsync(Socket socket)
        {
            var bytes = new List<byte>();
            byte[] buffer = new byte[1];
            while (true)
            {
                int received = await socket.ReceiveAsync(buffer, SocketFlags.None);
                if (received == 0) return null;
                if (buffer[0] == (byte)'\n')
                    break;
                bytes.Add(buffer[0]);
            }
            return Encoding.UTF8.GetString(bytes.ToArray());
        }

        private class OrderMessage
        {
            [System.Text.Json.Serialization.JsonPropertyName("orderId")]
            public int OrderId { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("userId")]
            public int UserId { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("deliveryType")]
            public string DeliveryType { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("address")]
            public string Address { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("createdAt")]
            public string CreatedAt { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("items")]
            public OrderItem[] Items { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("totalAmount")]
            public decimal TotalAmount { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("cardNumber")]
            public string CardNumber { get; set; }
        }

        private class OrderItem
        {
            [System.Text.Json.Serialization.JsonPropertyName("productId")]
            public string ProductId { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("name")]
            public string Name { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("quantity")]
            public int Quantity { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("price")]
            public decimal Price { get; set; }
        }
    }
}