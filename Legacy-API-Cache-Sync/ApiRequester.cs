using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using MySql.Data.MySqlClient;


namespace Legacy_API_Cache_Sync
{
    using System.Security.Policy;
    using System.Text.Json.Nodes;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal class ApiRequester
    {
        private static async Task Sync(string endpoint)
        {
            Logs.Logger.LogInformation("Request Data for {0} endpoint", (object)endpoint);
            HttpResponseMessage httpResponseMessage = ApiRequest(endpoint);
            if (httpResponseMessage.StatusCode != HttpStatusCode.OK)
            {
                Logs.Logger.LogError("Received non-ok response: {0}", (object)httpResponseMessage.StatusCode);
            }
            else
            {
                if (!httpResponseMessage.Content.Headers.ContentType.MediaType.ToLower().Contains("json"))
                {
                    Logs.Logger.LogError(
                        "Media Type is not JSON: {0}",
                        (object)httpResponseMessage.Content.Headers.ContentType.MediaType);
                }

                var result = await httpResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
                var path = Path.Combine(AppContext.BaseDirectory, "temp", Options.jsonPath, endpoint + ".json");
                await File.WriteAllTextAsync(path, result);

            }
        }

        public static async Task RestApiSync(CancellationToken stoppingToken)
        {
            foreach (string endpoint in Options.Endpoints)
            {
                await Sync(endpoint);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            await CharacterSyncAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            if(stoppingToken.IsCancellationRequested) return;
            await VehicleHoldsSync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            if (stoppingToken.IsCancellationRequested) return;
            await InmatesSync(stoppingToken);
            if (stoppingToken.IsCancellationRequested) return;
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await SavePdTimesAsync();
            if (stoppingToken.IsCancellationRequested) return;
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await SaveEmsTimesAsync();
            if (stoppingToken.IsCancellationRequested) return;
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await SaveItemsAsync();
            if (stoppingToken.IsCancellationRequested) return;
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await SaveVehicleAsync();
        }

        private static async Task CharacterSyncAsync(CancellationToken stoppingToken)
        {
            Logs.Logger.LogInformation("Syncing Characters From City");
                
            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.jsonPath}/characters/*/data,job,duty");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            await ProcessLargeJson(result, 10000, stoppingToken);
            Logs.Logger.LogInformation("Character Sync Done");
        }

        private static async Task VehicleHoldsSync(CancellationToken stoppingToken)
        {
            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var timestamp = DateTimeOffset.Now.ToUnixTimeSeconds();
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.jsonPath}/character_vehicles/police_impound>{timestamp}/data");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            var path = Path.Combine(AppContext.BaseDirectory, "temp", Options.jsonPath, "holds.json");
            await File.WriteAllTextAsync(path, result, stoppingToken);
            Logs.Logger.LogInformation("Holds Sync Done");
        }

        private static async Task InmatesSync(CancellationToken stoppingToken)
        {
            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var timestamp = DateTimeOffset.Now.ToUnixTimeSeconds();
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.jsonPath}/characters/jail>{timestamp}/data");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            var path = Path.Combine(AppContext.BaseDirectory, "temp", Options.jsonPath, "inmates.json");
            await File.WriteAllTextAsync(path, result, stoppingToken);
            Logs.Logger.LogInformation("Inmates Sync Done");
        }

        private static async Task ProcessLargeJson(string largeJsonString, int chunkSize,
            CancellationToken stoppingToken)
        {
            var jsonObject = JObject.Parse(largeJsonString);

            var dataArray = jsonObject["data"] as JArray;
            foreach (JObject dataObject in dataArray)
            {
                dataObject.Remove("backstory");
                if (stoppingToken.IsCancellationRequested) return;
            }

            var numChunks = (int)Math.Ceiling((double)dataArray.Count / chunkSize);

            for (int i = 0; i < numChunks; i++)
            {
                Console.WriteLine($"Processing Chunk {i} of {numChunks}");
                JArray chunkArray = new JArray();
                for (int j = i * chunkSize; j < Math.Min((i + 1) * chunkSize, dataArray.Count); j++)
                {
                    // Replace null values with empty strings and escape single quotes
                    string itemJsonString = dataArray[j].ToString().Replace(": null", ": \"\"").Replace("'", "''");
                    JObject itemObject = JObject.Parse(itemJsonString);

                    chunkArray.Add(itemObject);
                    if (stoppingToken.IsCancellationRequested) return;
                }

                string chunkJsonString = new JObject(new JProperty("data", chunkArray)).ToString();
                Console.WriteLine($"Chunk {i}");
                await SyncCharacterToMdt(chunkJsonString);
                if (stoppingToken.IsCancellationRequested) return;
            }

        }

        private static async Task SyncCharacterToMdt(string character_json)
        {

            try
            {

                Console.WriteLine($"Sync Start");
                var connectionString = new MySqlConnectionStringBuilder
                {
                    Server = Options.MySqlHost,
                    Port = 3306,
                    UserID = Options.MySqlUsername,
                    Password = Options.MySqlPassword,
                    Database = Options.MySqlDatabase
                };

                await using (MySqlConnection connection = new MySqlConnection(connectionString.ToString()))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand("InsertOrUpdateCharacter", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.Add(new MySqlParameter("character_json", MySqlDbType.LongText)).Value = character_json;

                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine("=====");
                Console.WriteLine(ex);
                Console.WriteLine("=====");
            }
        }

        private static HttpResponseMessage ApiRequest(string endpoint)
        {
            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.ApiUrl);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, "/op-framework/" + endpoint + ".json");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            return httpClient.Send(request);
        }

        private static async Task SavePdTimesAsync()
        {
            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.ApiUrl);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, $"{Options.MdtServer}/time/load");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            await httpClient.SendAsync(request);
            Logs.Logger.LogInformation("PD Time Sync Done");
        }

        private static async Task SaveEmsTimesAsync()
        {
            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.ApiUrl);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, $"{Options.MdtServer}/time/ems/test/load");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            await httpClient.SendAsync(request);
            Logs.Logger.LogInformation("EMS TIme Sync Done");
        }

        private static async Task SaveVehicleAsync()
        {
            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.ApiUrl);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, $"{Options.MdtServer}/vehicles/load");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            await httpClient.SendAsync(request);
            Logs.Logger.LogInformation("Vehicle Sync Done");
        }

        private static async Task SaveItemsAsync()
        {
            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.ApiUrl);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, $"{Options.MdtServer}/items/load");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            await httpClient.SendAsync(request);
            Logs.Logger.LogInformation("Vehicle Sync Done");
        }
    }
}
