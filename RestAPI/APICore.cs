using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;

namespace RestAPI
{
    public class APICore
    {
        private readonly IPAddress ipAPI = IPAddress.Parse(Environment.GetEnvironmentVariable("API_LISTEN_HOST") ?? "127.0.0.2");
        private readonly int portAPI = 5001;
        private Socket acceptClients;
        private bool isAlive = false;

        private readonly IPAddress brokerIP = IPAddress.Parse("127.0.0.3");
        private readonly int brokerPort = 5002;

        private readonly string connectionString;

        public APICore(string[] args)
        {
            connectionString = Environment.GetEnvironmentVariable("PG_CONNECTION")
                ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=stalker";
        }

        public async Task StartWorking()
        {
            _ = Task.Run(() => BackgroundPublisher());

            acceptClients = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                acceptClients.Bind(new IPEndPoint(ipAPI, portAPI));
                acceptClients.Listen(100);
                isAlive = true;
                Console.WriteLine($"REST API запущен на {ipAPI}:{portAPI}");

                while (isAlive)
                {
                    Socket client = await acceptClients.AcceptAsync();
                    _ = HandleClientAsync(client);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка сервера API: {ex.Message}");
            }
        }

        private async Task HandleClientAsync(Socket client)
        {
            try
            {
                await ReadHTTPFromClient(client);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Ошибка при обработке клиента: {e.Message}");
            }
            finally
            {
                try { client.Shutdown(SocketShutdown.Both); } catch { }
                client.Close();
            }
        }

        public async Task ReadHTTPFromClient(Socket client)
        {
            byte[] buffer = new byte[8192];
            List<byte> data = new List<byte>();
            int sizeBody = -1;
            int startBody = -1;
            while (isAlive)
            {
                int len = await client.ReceiveAsync(buffer);
                if (len == 0) break;

                data.AddRange(buffer.Take(len));
                if (IsFullHttp(data.ToArray(), ref startBody, ref sizeBody))
                {
                    byte[] answer = await ParseHTTP(data.ToArray(), startBody, sizeBody);
                    await SendAnswer(answer, client);
                    data.Clear();
                    sizeBody = -1;
                    startBody = -1;
                }
            }
        }

        public bool IsFullHttp(byte[] data, ref int startBody, ref int sizeBody)
        {
            string http = Encoding.ASCII.GetString(data);
            if (startBody == -1)
            {
                startBody = http.IndexOf("\r\n\r\n");
                if (startBody == -1) return false;
                startBody += 4;
            }
            if (sizeBody == -1)
            {
                sizeBody = 0;
                string[] parts = http.Split("\r\n");
                foreach (string part in parts)
                {
                    if (part.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                    {
                        int pos = part.IndexOf(':');
                        string value = part.Substring(pos + 1).Trim();
                        sizeBody = int.Parse(value);
                        break;
                    }
                }
            }
            return data.Length >= sizeBody + startBody;
        }

        public async Task<byte[]> ParseHTTP(byte[] data, int startBody, int sizeBody)
        {
            string headerString = Encoding.ASCII.GetString(data, 0, startBody - 4);
            string[] lines = headerString.Split("\r\n");
            if (lines.Length < 1)
                return BuildErrorResponse(400, "Bad Request");

            string[] requestLine = lines[0].Split(' ', 3);
            if (requestLine.Length < 3)
                return BuildErrorResponse(400, "Bad Request");

            string method = requestLine[0].ToUpperInvariant();
            string pathAndQuery = requestLine[1];
            string path = pathAndQuery.Split('?')[0];

            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 1; i < lines.Length; i++)
            {
                int colonPos = lines[i].IndexOf(':');
                if (colonPos > 0)
                {
                    string name = lines[i].Substring(0, colonPos).Trim();
                    string value = lines[i].Substring(colonPos + 1).Trim();
                    headers[name] = value;
                }
            }

            byte[] body = new byte[sizeBody];
            if (sizeBody > 0)
                Array.Copy(data, startBody, body, 0, sizeBody);

            return await ProcessHttpRequest(method, path, headers, body);
        }

        private async Task<byte[]> ProcessHttpRequest(string method, string path, Dictionary<string, string> headers, byte[] body)
        {
            try
            {
                if (path == "/api/orders" && method == "POST")
                    return await HandleCreateOrder(body);

                if (path.StartsWith("/api/orders/") && method == "GET")
                {
                    string orderId = path.Substring("/api/orders/".Length);
                    return await HandleGetOrder(orderId);
                }

                return BuildErrorResponse(404, "Not Found");
            }
            catch (JsonException)
            {
                return BuildErrorResponse(400, "Invalid JSON");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка обработки запроса: {ex.Message}");
                return BuildErrorResponse(500, "Internal Server Error");
            }
        }

        private async Task<byte[]> HandleCreateOrder(byte[] body)
        {
            var req = JsonSerializer.Deserialize<CreateOrderFullRequest>(body);
            if (req == null || string.IsNullOrWhiteSpace(req.Email) || string.IsNullOrWhiteSpace(req.Password))
                return BuildErrorResponse(400, "Email and password required");
            if (string.IsNullOrWhiteSpace(req.CardNumber))
                return BuildErrorResponse(400, "Card number is required");
            if (req.Items == null || req.Items.Count == 0)
                return BuildErrorResponse(400, "Order must contain at least one item");
            if (string.IsNullOrWhiteSpace(req.Address))
                return BuildErrorResponse(400, "Address is required");

            int? userId = await GetUserIdByCredentials(req.Email, req.Password);
            if (userId == null)
            {
                if (await UserExists(req.Email))
                    return BuildErrorResponse(401, "Invalid email or password");
                userId = await CreateUser(req.Email, req.Password);
            }

            DateTime createdAt = DateTime.UtcNow;
            int orderId = await SaveOrderToDatabase(userId.Value, req, createdAt, published: false);

            decimal totalAmount = Math.Round(req.Items.Sum(i => i.Price * i.Quantity), 2);
            bool published = await PublishWithRetryAsync(orderId, userId.Value, req, createdAt, totalAmount);
            if (published)
                await MarkOrderAsPublished(orderId);

            var response = new
            {
                orderId = orderId,
                status = "accepted",
                createdAt = createdAt.ToString("o")
            };
            return BuildJsonResponse(202, response);
        }

        private async Task<byte[]> HandleGetOrder(string orderIdStr)
        {
            if (!int.TryParse(orderIdStr, out int orderId))
                return BuildErrorResponse(400, "Invalid order ID");

            var order = await GetOrderFromDatabase(orderId);
            if (order == null)
                return BuildErrorResponse(404, "Order not found");

            return BuildJsonResponse(200, order);
        }

        // ================== Работа с PostgreSQL ==================
        private async Task<int> SaveOrderToDatabase(int userId, CreateOrderFullRequest request, DateTime createdAt, bool published)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            string itemsJson = JsonSerializer.Serialize(request.Items);
            await using var cmd = new NpgsqlCommand(
                @"INSERT INTO orders (user_id, status, published_to_queue, items, delivery_type, address, created_at, updated_at)
                  VALUES (@userId, 'accepted', @published, @items::jsonb, @deliveryType, @address, @createdAt, @createdAt)
                  RETURNING order_id", conn);

            cmd.Parameters.AddWithValue("userId", userId);
            cmd.Parameters.AddWithValue("published", published);
            cmd.Parameters.AddWithValue("items", itemsJson);
            cmd.Parameters.AddWithValue("deliveryType", request.DeliveryType ?? "standard");
            cmd.Parameters.AddWithValue("address", request.Address);
            cmd.Parameters.AddWithValue("createdAt", createdAt);
            return (int)await cmd.ExecuteScalarAsync();
        }

        private async Task MarkOrderAsPublished(int orderId)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "UPDATE orders SET published_to_queue = TRUE WHERE order_id = @orderId", conn);
            cmd.Parameters.AddWithValue("orderId", orderId);
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task<object?> GetOrderFromDatabase(int orderId)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT order_id, status, created_at, updated_at, items, delivery_type, address FROM orders WHERE order_id = @orderId", conn);
            cmd.Parameters.AddWithValue("orderId", orderId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                string itemsJson = reader.GetString(4);
                return new
                {
                    orderId = reader.GetInt32(0),
                    status = reader.GetString(1),
                    createdAt = reader.GetDateTime(2).ToString("o"),
                    updatedAt = reader.GetDateTime(3).ToString("o"),
                    items = JsonSerializer.Deserialize<object>(itemsJson),
                    deliveryType = reader.GetString(5),
                    address = reader.GetString(6)
                };
            }
            return null;
        }

        private byte[] GenerateSalt(int length = 32)
        {
            byte[] salt = new byte[length];
            using var rng = RandomNumberGenerator.Create();
            rng.GetBytes(salt);
            return salt;
        }

        private byte[] ComputeHashWithSalt(string password, byte[] salt)
        {
            byte[] passwordBytes = Encoding.UTF8.GetBytes(password);
            byte[] combined = new byte[passwordBytes.Length + salt.Length];
            Buffer.BlockCopy(passwordBytes, 0, combined, 0, passwordBytes.Length);
            Buffer.BlockCopy(salt, 0, combined, passwordBytes.Length, salt.Length);
            using var sha = SHA256.Create();
            return sha.ComputeHash(combined);
        }

        private async Task<bool> UserExists(string email)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS(SELECT 1 FROM users WHERE LOWER(email) = LOWER(@email))", conn);
            cmd.Parameters.AddWithValue("email", email);
            return (bool)await cmd.ExecuteScalarAsync();
        }

        private async Task<int> CreateUser(string email, string password)
        {
            byte[] salt = GenerateSalt();
            byte[] hash = ComputeHashWithSalt(password, salt);
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO users (email, password_hash, salt) VALUES (@email, @hash, @salt) RETURNING id", conn);
            cmd.Parameters.AddWithValue("email", email);
            cmd.Parameters.AddWithValue("hash", hash);
            cmd.Parameters.AddWithValue("salt", salt);
            return (int)await cmd.ExecuteScalarAsync();
        }

        private async Task<int?> GetUserIdByCredentials(string email, string password)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT id, password_hash, salt FROM users WHERE LOWER(email) = LOWER(@email)", conn);
            cmd.Parameters.AddWithValue("email", email);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                int userId = reader.GetInt32(0);
                byte[] storedHash = (byte[])reader[1];
                byte[] storedSalt = (byte[])reader[2];
                byte[] computedHash = ComputeHashWithSalt(password, storedSalt);
                if (computedHash.SequenceEqual(storedHash))
                    return userId;
            }
            return null;
        }

        private async Task<bool> PublishWithRetryAsync(int orderId, int userId, CreateOrderFullRequest orderReq, DateTime createdAt, decimal totalAmount, int maxRetries = 3)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    await socket.ConnectAsync(new IPEndPoint(brokerIP, brokerPort));

                    var message = new
                    {
                        orderId = orderId,
                        userId = userId,
                        items = orderReq.Items,
                        deliveryType = orderReq.DeliveryType,
                        address = orderReq.Address,
                        createdAt = createdAt.ToString("o"),
                        totalAmount = totalAmount,
                        cardNumber = orderReq.CardNumber
                    };
                    string messageJson = JsonSerializer.Serialize(message);

                    byte[] cmdBytes = Encoding.UTF8.GetBytes("PUBLISH orders.new\n");
                    await socket.SendAsync(cmdBytes, SocketFlags.None);
                    byte[] jsonBytes = Encoding.UTF8.GetBytes(messageJson + "\n");
                    await socket.SendAsync(jsonBytes, SocketFlags.None);

                    string? response = await ReceiveLineAsync(socket);
                    if (response != null && response.StartsWith("OK", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"Заказ {orderId} опубликован (попытка {attempt})");
                        return true;
                    }
                    Console.WriteLine($"Неудачный ответ брокера: {response}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка публикации заказа {orderId}, попытка {attempt}: {ex.Message}");
                }

                if (attempt < maxRetries)
                    await Task.Delay(TimeSpan.FromSeconds(2 * attempt));
            }
            Console.WriteLine($"Не удалось опубликовать заказ {orderId} после {maxRetries} попыток");
            return false;
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

        private async Task BackgroundPublisher()
        {
            while (true)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30));
                    var unpublishedOrders = await GetUnpublishedOrders();

                    foreach (var order in unpublishedOrders)
                    {
                        var request = new CreateOrderFullRequest
                        {
                            Email = "",
                            Password = "",
                            Items = JsonSerializer.Deserialize<List<OrderItem>>(order.ItemsJson),
                            DeliveryType = order.DeliveryType,
                            Address = order.Address
                        };

                        decimal totalAmount = request.Items.Sum(i => i.Price * i.Quantity);
                        bool published = await PublishWithRetryAsync(order.OrderId, order.UserId, request, order.CreatedAt, totalAmount);
                        if (published)
                            await MarkOrderAsPublished(order.OrderId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка в фоновом компенсаторе: {ex.Message}");
                }
            }
        }

        private class UnpublishedOrder
        {
            public int OrderId { get; set; }
            public int UserId { get; set; }
            public string ItemsJson { get; set; }
            public string DeliveryType { get; set; }
            public string Address { get; set; }
            public DateTime CreatedAt { get; set; }
        }

        private async Task<List<UnpublishedOrder>> GetUnpublishedOrders()
        {
            var list = new List<UnpublishedOrder>();
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                @"SELECT order_id, user_id, items::text, delivery_type, address, created_at 
                  FROM orders 
                  WHERE status = 'accepted' AND published_to_queue = FALSE 
                  ORDER BY created_at", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new UnpublishedOrder
                {
                    OrderId = reader.GetInt32(0),
                    UserId = reader.GetInt32(1),
                    ItemsJson = reader.GetString(2),
                    DeliveryType = reader.GetString(3),
                    Address = reader.GetString(4),
                    CreatedAt = reader.GetDateTime(5)
                });
            }
            return list;
        }

        private byte[] BuildErrorResponse(int statusCode, string message)
        {
            var errorObj = new { error = message };
            return BuildJsonResponse(statusCode, errorObj);
        }

        private byte[] BuildJsonResponse(int statusCode, object data)
        {
            string json = JsonSerializer.Serialize(data);
            byte[] bodyBytes = Encoding.UTF8.GetBytes(json);
            string statusText = statusCode switch
            {
                200 => "OK",
                201 => "Created",
                202 => "Accepted",
                400 => "Bad Request",
                401 => "Unauthorized",
                404 => "Not Found",
                500 => "Internal Server Error",
                _ => "Unknown"
            };
            string headers = $"HTTP/1.1 {statusCode} {statusText}\r\n" +
                             "Content-Type: application/json\r\n" +
                             $"Content-Length: {bodyBytes.Length}\r\n" +
                             "Connection: close\r\n\r\n";
            byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
            byte[] fullResponse = new byte[headerBytes.Length + bodyBytes.Length];
            Array.Copy(headerBytes, 0, fullResponse, 0, headerBytes.Length);
            Array.Copy(bodyBytes, 0, fullResponse, headerBytes.Length, bodyBytes.Length);
            return fullResponse;
        }

        private async Task SendAnswer(byte[] answer, Socket socket)
        {
            await socket.SendAsync(answer, SocketFlags.None);
        }

        public class CreateOrderFullRequest
        {
            [System.Text.Json.Serialization.JsonPropertyName("email")]
            public string Email { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("password")]
            public string Password { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("cardNumber")]
            public string CardNumber { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("items")]
            public List<OrderItem> Items { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("deliveryType")]
            public string DeliveryType { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("address")]
            public string Address { get; set; }
        }

        public class OrderItem
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