namespace CardService
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            CardServiceCore core = new CardServiceCore(args);
            await core.StartWorking();
        }
    }
}
