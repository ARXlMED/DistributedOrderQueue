using Shared;
using System.Net;
using System.Net.Sockets;

namespace RestAPI
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            APICore core = new APICore(args);
            await core.StartWorking();
        }
    }
}
