using Microsoft.CSharp.RuntimeBinder;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Legacy_API_Cache_Sync
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            Logs.Logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            SetOptions();
            if (!Directory.Exists(Path.Combine(AppContext.BaseDirectory, "temp")))
                Directory.CreateDirectory(Path.Combine(AppContext.BaseDirectory, "temp"));

            if (Options.jsonPath != "" && !Directory.Exists(Path.Combine(AppContext.BaseDirectory, "temp",Options.jsonPath)))
                Directory.CreateDirectory(Path.Combine(AppContext.BaseDirectory, "temp", Options.jsonPath));

            var tasks = new List<Task>();

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTime.Now);
                Logs.Logger.LogInformation("Sync running at: {time}", DateTime.Now);
                try
                {
                    tasks.Add(Task.Run(() => ApiRequester.RestApiSync(stoppingToken), stoppingToken));
                    tasks.Add(Task.Run(() => ApiRequester.GetApiData(),stoppingToken));
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
                catch (Exception ex)
                {
                    Logs.Logger.LogError(ex.Message);
                }
                Logs.Logger.LogInformation("Sync finished at: {time}, now sleeping for {duration} minutes", DateTime.Now, Options.ApiFreq);
                await Task.Delay((int)TimeSpan.FromMinutes(Options.ApiFreq).TotalMilliseconds, stoppingToken);
                
            }
        }

        private void SetOptions()
        {
            dynamic settings = JObject.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "appsettings.json")));
            Options.ApiUrl = settings.apiBaseUrl;
            Options.ApiKey = settings.apiKey;    
            Options.jsonPath = settings.jsonPath;
            Options.ApiFreq = settings.apiSleepDutation;
            Options.RestUrl = settings.restBaseUrl;
            Options.Endpoints = settings.endpoints;
            Options.MySqlHost = settings.mySql.host;
            Options.MySqlUsername = settings.mySql.user;
            Options.MySqlPassword = settings.mySql.password;
            Options.MySqlDatabase = settings.mySql.database;
        }
    }
}