namespace MessageBroker
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var core = new BrokerCore(args);
            await core.StartWorking();
        }
    }
}
