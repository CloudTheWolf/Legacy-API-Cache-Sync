namespace Legacy_API_Cache_Sync
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddHostedService<Worker>();
                })
                .UseSystemd()
                .UseWindowsService()
                .Build();

            host.Run();
        }
    }
}