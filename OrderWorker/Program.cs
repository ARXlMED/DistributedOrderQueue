namespace OrderWorker
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var core = new WorkerCore(args);
            await core.StartWorking();
            Console.ReadLine();
        }
    }
}
