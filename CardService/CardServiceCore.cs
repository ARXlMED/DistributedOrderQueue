using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace CardService
{
    public class CardServiceCore
    {
        private readonly IPAddress ip = IPAddress.Parse(Environment.GetEnvironmentVariable("CARD_LISTEN_HOST") ?? "127.0.0.4");
        private readonly int port = 6000;
        private readonly string connectionString;
        private Socket serverSocket;
        private bool isAlive = false;

        public CardServiceCore(string[] args)
        {
            connectionString = Environment.GetEnvironmentVariable("CARD_PG_CONNECTION")
                ?? "Host=localhost;Port=5432;Database=carddb;Username=postgres;Password=stalker";
        }

        public async Task StartWorking()
        {
            await InitializeDatabaseAsync();

            serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            serverSocket.Bind(new IPEndPoint(ip, port));
            serverSocket.Listen(100);
            isAlive = true;
            Console.WriteLine($"CardService запущен на {ip}:{port}");

            while (isAlive)
            {
                Socket client = await serverSocket.AcceptAsync();
                _ = HandleClientAsync(client);
            }
        }

        private async Task InitializeDatabaseAsync()
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(@"
                CREATE TABLE IF NOT EXISTS cards (
                    user_id INTEGER PRIMARY KEY,
                    card_number TEXT NOT NULL,
                    balance NUMERIC NOT NULL DEFAULT 10000.00
                );
                INSERT INTO cards (user_id, card_number, balance) VALUES (1, '1234567812345678', 10000.00)
                ON CONFLICT (user_id) DO NOTHING;
            ", conn);
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task HandleClientAsync(Socket client)
        {
            try
            {
                while (true)
                {
                    string? line = await ReceiveLineAsync(client);
                    if (line == null) break;

                    string[] parts = line.Split(' ');
                    if (parts.Length == 0) continue;

                    string command = parts[0].ToUpperInvariant();
                    string response;

                    switch (command)
                    {
                        case "CHARGE":
                            if (parts.Length < 4)
                            { response = "ERROR Invalid arguments"; break; }
                            response = await ChargeAsync(parts[1], parts[2], parts[3]);
                            break;
                        case "BALANCE":
                            if (parts.Length < 2)
                            { response = "ERROR Missing userId"; break; }
                            response = await GetBalanceAsync(parts[1]);
                            break;
                        default:
                            response = "ERROR Unknown command";
                            break;
                    }

                    byte[] respBytes = Encoding.UTF8.GetBytes(response + "\n");
                    await client.SendAsync(respBytes, SocketFlags.None);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CardService client error: {ex.Message}");
            }
            finally
            {
                try { client.Shutdown(SocketShutdown.Both); } catch { }
                client.Close();
            }
        }

        private async Task<string> ChargeAsync(string userIdStr, string amountStr, string cardNumber)
        {
            Console.WriteLine($"[CARD] CHARGE запрос: userIdStr={userIdStr}, amountStr={amountStr}, cardNumber={cardNumber}");
            if (!int.TryParse(userIdStr, out int userId) || !decimal.TryParse(amountStr, out decimal amount))
            {
                Console.WriteLine($"[CARD] Ошибка парсинга чисел");
                return "ERROR Invalid arguments";
            }

            Console.WriteLine($"[CARD] Открываю соединение с БД");
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            Console.WriteLine($"[CARD] Соединение открыто");

            await using var upsertCmd = new NpgsqlCommand(@"INSERT INTO cards (user_id, card_number, balance) VALUES (@userId, @cardNumber, 100000) ON CONFLICT (user_id) DO UPDATE SET card_number = @cardNumber", conn);
            upsertCmd.Parameters.AddWithValue("userId", userId);
            upsertCmd.Parameters.AddWithValue("cardNumber", cardNumber);
            Console.WriteLine($"[CARD] Выполняю UPSERT для userId={userId}");
            await upsertCmd.ExecuteNonQueryAsync();
            Console.WriteLine($"[CARD] UPSERT выполнен");

            await using var chargeCmd = new NpgsqlCommand(
                "UPDATE cards SET balance = balance - @amount WHERE user_id = @userId AND balance >= @amount",
                conn);
            chargeCmd.Parameters.AddWithValue("amount", amount);
            chargeCmd.Parameters.AddWithValue("userId", userId);
            Console.WriteLine($"[CARD] Пытаюсь списать {amount} с userId={userId}");
            int rows = await chargeCmd.ExecuteNonQueryAsync();
            Console.WriteLine($"[CARD] Списание: rowsAffected={rows}");

            return rows > 0 ? "OK" : "FAIL Insufficient funds";
        }

        private async Task<string> GetBalanceAsync(string userIdStr)
        {
            if (!int.TryParse(userIdStr, out int userId))
                return "ERROR Invalid userId";

            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT balance FROM cards WHERE user_id = @userId", conn);
            cmd.Parameters.AddWithValue("userId", userId);
            var result = await cmd.ExecuteScalarAsync();
            return result != null ? $"OK {result}" : "ERROR Card not found";
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
    }
}