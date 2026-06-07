using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Shared;

namespace WebServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string configPath = Path.Combine(AppContext.BaseDirectory, "appsettings.txt");
            string webHost = ConfigLoader.Get(configPath, "WEB_SERVER_HOST", "127.0.0.1");
            int webPort = int.Parse(ConfigLoader.Get(configPath, "WEB_SERVER_PORT", "5000"));
            string apiHost = ConfigLoader.Get(configPath, "API_HOST", "127.0.0.2");
            int apiPort = int.Parse(ConfigLoader.Get(configPath, "API_PORT", "5001"));

            string apiBase = $"http://{apiHost}:{apiPort}";

            string htmlPath = Path.Combine(AppContext.BaseDirectory, "index.html");
            if (!File.Exists(htmlPath))
            {
                Console.WriteLine($"Файл index.html не найден по пути: {htmlPath}");
                return;
            }
            string htmlTemplate = await File.ReadAllTextAsync(htmlPath);
            string finalHtml = htmlTemplate.Replace("__API_BASE__", apiBase);
            byte[] fileBytes = Encoding.UTF8.GetBytes(finalHtml);

            var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(new IPEndPoint(IPAddress.Parse(webHost), webPort));
            listener.Listen(10);
            Console.WriteLine($"Веб-сервер запущен на {webHost}:{webPort}");
            Console.WriteLine($"API будет доступен по адресу: {apiBase}");

            while (true)
            {
                Socket client = await listener.AcceptAsync();
                _ = HandleClientAsync(client, fileBytes);
            }
        }

        static async Task HandleClientAsync(Socket client, byte[] fileBytes)
        {
            try
            {
                byte[] buffer = new byte[1024];
                int received = await client.ReceiveAsync(buffer);
                if (received == 0) return;

                string headers = "HTTP/1.1 200 OK\r\n" +
                                 "Content-Type: text/html; charset=utf-8\r\n" +
                                 $"Content-Length: {fileBytes.Length}\r\n" +
                                 "Connection: close\r\n\r\n";
                byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
                byte[] fullResponse = new byte[headerBytes.Length + fileBytes.Length];
                Array.Copy(headerBytes, 0, fullResponse, 0, headerBytes.Length);
                Array.Copy(fileBytes, 0, fullResponse, headerBytes.Length, fileBytes.Length);

                await client.SendAsync(fullResponse);
            }
            catch { }
            finally
            {
                try { client.Shutdown(SocketShutdown.Both); } catch { }
                client.Close();
            }
        }
    }
}