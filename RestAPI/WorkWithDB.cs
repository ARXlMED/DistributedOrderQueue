using Npgsql;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using static RestAPI.APICore;

namespace RestAPI
{
    public static class WorkWithDB
    {
        public static async Task<int> SaveOrderToDatabase(string connectionString, int userId, List<OrderItem> items, string deliveryType, string address, DateTime createdAt, bool published, string cardNumber)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            string itemsJson = JsonSerializer.Serialize(items);
            await using var cmd = new NpgsqlCommand(@"INSERT INTO orders (user_id, status, published_to_queue, items, delivery_type, address, created_at, updated_at, card_number)
            VALUES (@userId, 'accepted', @published, @items::jsonb, @deliveryType, @address, @createdAt, @createdAt, @cardNumber) RETURNING order_id", conn);

            cmd.Parameters.AddWithValue("userId", userId);
            cmd.Parameters.AddWithValue("published", published);
            cmd.Parameters.AddWithValue("items", itemsJson);
            cmd.Parameters.AddWithValue("deliveryType", deliveryType ?? "standard");
            cmd.Parameters.AddWithValue("address", address);
            cmd.Parameters.AddWithValue("createdAt", createdAt);
            cmd.Parameters.AddWithValue("cardNumber", cardNumber);
            return (int)await cmd.ExecuteScalarAsync();
        }


        public static async Task<OrderItem?> GetProductById(string connectionString, int productId)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT product_id, name, price FROM products WHERE product_id = @productId", conn);
            cmd.Parameters.AddWithValue("productId", productId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new OrderItem
                {
                    ProductId = reader.GetInt32(0),
                    Name = reader.GetString(1),
                    Price = reader.GetDecimal(2)
                };
            }
            return null;
        }

        public static async Task MarkOrderAsPublished(string connectionString, int orderId)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "UPDATE orders SET published_to_queue = TRUE WHERE order_id = @orderId", conn);
            cmd.Parameters.AddWithValue("orderId", orderId);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<object?> GetOrderFromDatabase(string connectionString, int orderId)
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

        public static byte[] GenerateSalt(int length = 32)
        {
            byte[] salt = new byte[length];
            using var rng = RandomNumberGenerator.Create();
            rng.GetBytes(salt);
            return salt;
        }

        public static byte[] ComputeHashWithSalt(string password, byte[] salt)
        {
            byte[] passwordBytes = Encoding.UTF8.GetBytes(password);
            byte[] combined = new byte[passwordBytes.Length + salt.Length];
            Buffer.BlockCopy(passwordBytes, 0, combined, 0, passwordBytes.Length);
            Buffer.BlockCopy(salt, 0, combined, passwordBytes.Length, salt.Length);
            using var sha = SHA256.Create();
            return sha.ComputeHash(combined);
        }

        public static async Task<bool> UserExists(string connectionString, string email)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS(SELECT 1 FROM users WHERE LOWER(email) = LOWER(@email))", conn);
            cmd.Parameters.AddWithValue("email", email);
            return (bool)await cmd.ExecuteScalarAsync();
        }

        public static async Task<int> CreateUser(string connectionString, string email, string password)
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

        public static async Task<int?> GetUserIdByCredentials(string connectionString, string email, string password)
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

        public class UnpublishedOrder
        {
            public int OrderId { get; set; }
            public int UserId { get; set; }
            public string ItemsJson { get; set; }
            public string DeliveryType { get; set; }
            public string Address { get; set; }
            public DateTime CreatedAt { get; set; }
            public string CardNumber { get; set; }

        }

        public static async Task<List<UnpublishedOrder>> GetUnpublishedOrders(string connectionString)
        {
            var list = new List<UnpublishedOrder>();
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                @"SELECT order_id, user_id, items::text, delivery_type, address, created_at, card_number
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
                    CreatedAt = reader.GetDateTime(5),
                    CardNumber = reader.GetString(6)
                });
            }
            return list;
        }
    }
}
