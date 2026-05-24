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
using Shared;

namespace RestAPI
{
    public class APICore
    {
        IPAddress ipAPI = IPAddress.Parse(Environment.GetEnvironmentVariable("API_HOST") ?? "127.0.0.2");
        int portAPI = int.Parse(Environment.GetEnvironmentVariable("API_PORT") ?? "5001");
        Socket acceptClients;
        bool isAlive = false;

        IPAddress brokerIP = IPAddress.Parse(Environment.GetEnvironmentVariable("BROKER_HOST") ?? "127.0.0.3");
        int brokerPort = int.Parse(Environment.GetEnvironmentVariable("BROKER_PORT") ?? "5002");

        string connectionString;

        public APICore(string[] args)
        {
            string configPath = Path.Combine(AppContext.BaseDirectory, "appsettings.txt");
            ipAPI = IPAddress.Parse(ConfigLoader.Get(configPath, "API_HOST", "127.0.0.2"));
            portAPI = int.Parse(ConfigLoader.Get(configPath, "API_PORT", "5001"));
            brokerIP = IPAddress.Parse(ConfigLoader.Get(configPath, "BROKER_HOST", "127.0.0.3"));
            brokerPort = int.Parse(ConfigLoader.Get(configPath, "BROKER_PORT", "5002"));
            connectionString = ConfigLoader.Get(configPath, "PG_CONNECTION", "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=stalker");
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
                if (method == "OPTIONS")
                    return BuildCorsPreflightResponse(headers);

                if (path == "/api/products" && method == "GET")
                    return await HandleGetProducts();

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

        private byte[] BuildCorsPreflightResponse(Dictionary<string, string> headers)
        {
            string responseHeaders =
                "HTTP/1.1 204 No Content\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n" +
                "Access-Control-Allow-Headers: Content-Type, Authorization\r\n" +
                "Access-Control-Max-Age: 86400\r\n" +
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            return Encoding.ASCII.GetBytes(responseHeaders);
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

            int? userId = await WorkWithDB.GetUserIdByCredentials(connectionString, req.Email, req.Password);
            if (userId == null)
            {
                if (await WorkWithDB.UserExists(connectionString, req.Email))
                    return BuildErrorResponse(401, "Invalid email or password");
                userId = await WorkWithDB.CreateUser(connectionString, req.Email, req.Password);
            }

            var fullItems = new List<OrderItem>();
            foreach (var item in req.Items)
            {
                var product = await WorkWithDB.GetProductById(connectionString, item.ProductId);
                if (product == null)
                    return BuildErrorResponse(400, $"Product with ID '{item.ProductId}' not found");
                product.Quantity = item.Quantity;
                fullItems.Add(product);
            }

            DateTime createdAt = DateTime.UtcNow;
            int orderId = await WorkWithDB.SaveOrderToDatabase(connectionString, userId.Value, fullItems, req.DeliveryType, req.Address, createdAt, published: false, req.CardNumber);

            decimal totalAmount = Math.Round(fullItems.Sum(i => i.Price * i.Quantity), 2);
            bool published = await PublishWithRetryAsync(orderId, userId.Value, fullItems, req.DeliveryType, req.Address, createdAt, totalAmount, req.CardNumber);
            if (published)
                await WorkWithDB.MarkOrderAsPublished(connectionString, orderId);

            var response = new
            {
                orderId = orderId,
                status = "accepted",
                createdAt = createdAt.ToString("o")
            };
            return BuildJsonResponse(202, response);
        }

        private async Task<byte[]> HandleGetProducts()
        {
            var products = new List<object>();
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT product_id, name FROM products ORDER BY name", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                products.Add(new
                {
                    productId = reader.GetInt32(0),
                    name = reader.GetString(1)
                });
            }
            return BuildJsonResponse(200, products);
        }

        private async Task<byte[]> HandleGetOrder(string orderIdStr)
        {
            if (!int.TryParse(orderIdStr, out int orderId))
                return BuildErrorResponse(400, "Invalid order ID");

            var order = await WorkWithDB.GetOrderFromDatabase(connectionString, orderId);
            if (order == null)
                return BuildErrorResponse(404, "Order not found");

            return BuildJsonResponse(200, order);
        }

        

        private async Task<bool> PublishWithRetryAsync(int orderId, int userId, List<OrderItem> items, string deliveryType, string address, DateTime createdAt, decimal totalAmount, string cardNumber, int maxRetries = 3)
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
                        items = items,
                        deliveryType = deliveryType,
                        address = address,
                        createdAt = createdAt.ToString("o"),
                        totalAmount = totalAmount,
                        cardNumber = cardNumber
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
                    var unpublishedOrders = await WorkWithDB.GetUnpublishedOrders(connectionString);

                    foreach (var order in unpublishedOrders)
                    {
                        var items = JsonSerializer.Deserialize<List<OrderItem>>(order.ItemsJson);
                        decimal totalAmount = items.Sum(i => i.Price * i.Quantity);
                        bool published = await PublishWithRetryAsync(order.OrderId, order.UserId, items, order.DeliveryType, order.Address, order.CreatedAt, totalAmount, order.CardNumber);
                        if (published)
                            await WorkWithDB.MarkOrderAsPublished(connectionString, order.OrderId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка в фоновом компенсаторе: {ex.Message}");
                }
            }
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
            string headers = $"HTTP/1.1 {statusCode} {statusText}\r\n" + "Content-Type: application/json\r\n" + "Access-Control-Allow-Origin: *\r\n" + $"Content-Length: {bodyBytes.Length}\r\n" + "Connection: close\r\n\r\n";
            byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
            byte[] fullResponse = new byte[headerBytes.Length + bodyBytes.Length];
            Array.Copy(headerBytes, 0, fullResponse, 0, headerBytes.Length);
            Array.Copy(bodyBytes, 0, fullResponse, headerBytes.Length, bodyBytes.Length);
            return fullResponse;
        }

        private async Task SendAnswer(byte[] answer, Socket socket)
        {
            await socket.SendAsync(answer);
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
            public List<CreateOrderItemRequest> Items { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("deliveryType")]
            public string DeliveryType { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("address")]
            public string Address { get; set; }
        }

        public class CreateOrderItemRequest
        {
            [System.Text.Json.Serialization.JsonPropertyName("productId")]
            public int ProductId { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("quantity")]
            public int Quantity { get; set; }
        }

        public class OrderItem
        {
            [System.Text.Json.Serialization.JsonPropertyName("productId")]
            public int ProductId { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("name")]
            public string Name { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("quantity")]
            public int Quantity { get; set; }
            [System.Text.Json.Serialization.JsonPropertyName("price")]
            public decimal Price { get; set; }
        }
    }
}