using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageBroker
{
    public class BrokerCore
    {
        private readonly IPAddress ipBroker = IPAddress.Parse(Environment.GetEnvironmentVariable("BROKER_LISTEN_HOST") ?? "127.0.0.3");
        private readonly int portBroker = 5002;
        private Socket serverSocket;
        private bool isAlive = false;

        private readonly ConcurrentDictionary<string, List<Message>> topics = new();
        private readonly object locker = new();

        private readonly TimeSpan visibilityTimeout = TimeSpan.FromSeconds(60);
        private readonly string journalPath = Path.Combine(AppContext.BaseDirectory, "broker_journal.log");
        private readonly long maxJournalSize = 10L * 1024 * 1024;

        private CancellationTokenSource cts;

        public BrokerCore(string[] args) { }

        public async Task StartWorking()
        {
            LoadJournal();

            cts = new CancellationTokenSource();
            _ = Task.Run(() => ProcessInvisibleMessages(cts.Token));

            serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                serverSocket.Bind(new IPEndPoint(ipBroker, portBroker));
                serverSocket.Listen(100);
                isAlive = true;
                Console.WriteLine($"Message Broker запущен на {ipBroker}:{portBroker}");

                while (isAlive)
                {
                    Socket client = await serverSocket.AcceptAsync();
                    _ = HandleClientAsync(client);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка сервера брокера: {ex.Message}");
            }
        }

        private void LoadJournal()
        {
            if (!File.Exists(journalPath)) return;

            string[] lines = File.ReadAllLines(journalPath);
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                if (line.StartsWith("PUBLISH "))
                {
                    string[] parts = line.Split(' ', 3);
                    if (parts.Length < 3) continue;
                    string topic = parts[1];
                    string messageId = parts[2];
                    if (i + 1 < lines.Length)
                    {
                        string body = lines[++i];
                        var msg = new Message
                        {
                            Id = messageId,
                            Body = body,
                            Status = MessageStatus.Available
                        };
                        lock (locker)
                        {
                            if (!topics.ContainsKey(topic))
                                topics[topic] = new List<Message>();
                            topics[topic].Add(msg);
                        }
                    }
                }
                else if (line.StartsWith("DELETE "))
                {
                    string[] parts = line.Split(' ', 3);
                    if (parts.Length < 3) continue;
                    string topic = parts[1];
                    string messageId = parts[2];
                    lock (locker)
                    {
                        if (topics.TryGetValue(topic, out var list))
                        {
                            list.RemoveAll(m => m.Id == messageId);
                        }
                    }
                }
            }
            Console.WriteLine("Журнал загружен, сообщения восстановлены.");
        }

        private async Task HandleClientAsync(Socket client)
        {
            try
            {
                while (true)
                {
                    string? commandLine = await ReceiveLineAsync(client);
                    if (commandLine == null) break;

                    string[] parts = commandLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length == 0) continue;

                    string command = parts[0].ToUpperInvariant();
                    string[] args = parts.Length > 1 ? parts[1..] : Array.Empty<string>();

                    string? body = null;
                    if (command == "PUBLISH")
                    {
                        body = await ReceiveLineAsync(client);
                        if (body == null) break;
                    }

                    string response = await ProcessCommand(command, args, body);
                    byte[] responseBytes = Encoding.UTF8.GetBytes(response + "\n");
                    await client.SendAsync(responseBytes, SocketFlags.None);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка обработки клиента: {ex.Message}");
            }
            finally
            {
                try { client.Shutdown(SocketShutdown.Both); } catch { }
                client.Close();
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

        private async Task<string> ProcessCommand(string command, string[] args, string? body)
        {
            switch (command)
            {
                case "PUBLISH":
                    if (args.Length < 1 || string.IsNullOrWhiteSpace(body))
                        return "ERROR Missing topic or body";
                    return PublishMessage(args[0], body);

                case "CONSUME":
                    if (args.Length < 3)
                        return "ERROR Missing arguments: topic consumerId timeout";
                    string topic = args[0];
                    string consumerId = args[1];
                    if (!int.TryParse(args[2], out int timeoutSec) || timeoutSec < 0)
                        return "ERROR Invalid timeout";
                    return await ConsumeMessage(topic, consumerId, timeoutSec);

                case "DELETE":
                    if (args.Length < 3)
                        return "ERROR Missing arguments: topic messageId consumerId";
                    return DeleteMessage(args[0], args[1], args[2]);

                default:
                    return "ERROR Unknown command";
            }
        }

        private string PublishMessage(string topic, string body)
        {
            string messageId = Guid.NewGuid().ToString();
            var message = new Message
            {
                Id = messageId,
                Body = body,
                Status = MessageStatus.Available
            };

            lock (locker)
            {
                if (!topics.ContainsKey(topic))
                    topics[topic] = new List<Message>();
                topics[topic].Add(message);
            }

            AppendJournal($"PUBLISH {topic} {messageId}");
            AppendJournal(body);

            return "OK";
        }

        private async Task<string> ConsumeMessage(string topic, string consumerId, int timeoutSec)
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(timeoutSec);

            while (DateTime.UtcNow < deadline)
            {
                Message? msg = null;
                lock (locker)
                {
                    if (topics.TryGetValue(topic, out var list))
                    {
                        foreach (var candidate in list)
                        {
                            if (candidate.Status == MessageStatus.Available ||
                                (candidate.Status == MessageStatus.Invisible && candidate.VisibilityDeadline <= DateTime.UtcNow))
                            {
                                msg = candidate;
                                break;
                            }
                        }
                    }
                }

                if (msg != null)
                {
                    lock (locker)
                    {
                        msg.Status = MessageStatus.Invisible;
                        msg.ConsumerId = consumerId;
                        msg.VisibilityDeadline = DateTime.UtcNow.Add(visibilityTimeout);
                    }
                    return $"MESSAGE {msg.Id}\n{msg.Body}";
                }

                int delayMs = Math.Min(500, (int)((deadline - DateTime.UtcNow).TotalMilliseconds));
                if (delayMs <= 0) break;
                await Task.Delay(delayMs);
            }

            return "NONE";
        }

        private string DeleteMessage(string topic, string messageId, string consumerId)
        {
            lock (locker)
            {
                if (topics.TryGetValue(topic, out var list))
                {
                    var msg = list.Find(m => m.Id == messageId);
                    if (msg != null && msg.Status == MessageStatus.Invisible && msg.ConsumerId == consumerId)
                    {
                        list.Remove(msg);
                        AppendJournal($"DELETE {topic} {messageId}");
                        return "OK";
                    }
                }
            }
            return "ERROR Message not found or not owned by consumer";
        }

        private void AppendJournal(string entry)
        {
            try
            {
                File.AppendAllText(journalPath, entry + Environment.NewLine);

                FileInfo fileInfo = new FileInfo(journalPath);
                if (fileInfo.Length > maxJournalSize)
                {
                    Console.WriteLine($"Размер журнала превышен ({fileInfo.Length} > {maxJournalSize}). Начинаем компактификацию...");
                    lock (locker)
                    {
                        CompactJournal();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка записи в журнал: {ex.Message}");
            }
        }

        private void CompactJournal()
        {
            try
            {
                string backupPath = journalPath + ".bak";
                if (File.Exists(journalPath))
                {
                    if (File.Exists(backupPath))
                        File.Delete(backupPath);
                    File.Copy(journalPath, backupPath);
                    Console.WriteLine($"Создана резервная копия журнала: {backupPath}");
                }

                File.WriteAllText(journalPath, "");

                lock (locker)
                {
                    foreach (var kvp in topics)
                    {
                        string topic = kvp.Key;
                        var list = kvp.Value;

                        foreach (var msg in list)
                        {
                            if (msg.Status == MessageStatus.Available || msg.Status == MessageStatus.Invisible)
                            {
                                File.AppendAllText(journalPath, $"PUBLISH {topic} {msg.Id}" + Environment.NewLine);
                                File.AppendAllText(journalPath, msg.Body + Environment.NewLine);
                            }
                        }
                    }
                }

                Console.WriteLine($"Журнал компактифицирован. Размер: {new FileInfo(journalPath).Length} байт");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка при компактификации журнала: {ex.Message}");
            }
        }

        private void ProcessInvisibleMessages(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                Thread.Sleep(2000);
                lock (locker)
                {
                    foreach (var kvp in topics)
                    {
                        var list = kvp.Value;
                        for (int i = list.Count - 1; i >= 0; i--)
                        {
                            var msg = list[i];
                            if (msg.Status == MessageStatus.Invisible && msg.VisibilityDeadline <= DateTime.UtcNow)
                            {
                                msg.Status = MessageStatus.Available;
                                msg.ConsumerId = null;
                            }
                        }
                    }
                }
            }
        }

        public enum MessageStatus
        {
            Available,
            Invisible,
            Deleted
        }

        public class Message
        {
            public string Id { get; set; }
            public string Body { get; set; }
            public MessageStatus Status { get; set; } = MessageStatus.Available;
            public string? ConsumerId { get; set; }
            public DateTime VisibilityDeadline { get; set; }
        }
    }
}