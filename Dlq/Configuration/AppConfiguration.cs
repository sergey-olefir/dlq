using System.IO;
using Microsoft.Extensions.Configuration;

namespace Dlq.Configuration
{
    internal interface IAppConfiguration
    {
        string ServiceBusConnectionString { get; set; }
        int BatchSize { get; set; }
    }

    internal class AppConfiguration : IAppConfiguration
    {
        static AppConfiguration()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddUserSecrets("65fcd2a4-74c5-4a42-ad18-3bb53d2b8368")
                .Build();
            Instance = config.GetSection("application").Get<AppConfiguration>();
        }

        public static AppConfiguration Instance { get; }

        public string ServiceBusConnectionString { get; set; } = null!;
        
        public int BatchSize { get; set; }
    }
}