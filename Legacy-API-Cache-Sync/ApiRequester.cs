using System.Data;
using System.Net;
using System.Numerics;
using System.Text;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Legacy_API_Cache_Sync
{

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
            await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);
            if (stoppingToken.IsCancellationRequested) return;
            await VehicleSyncAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);
            if (stoppingToken.IsCancellationRequested) return;            
            await VehicleFinanceSync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);
            if (stoppingToken.IsCancellationRequested) return;
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
            var request = new HttpRequestMessage(HttpMethod.Get, $"{Options.serviceId}/" +
                $"characters?select=character_id,first_name,last_name,gender,job_name,department_name,position_name,date_of_birth,phone_number,license_identifier,mugshot_url");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            await ProcessLargeJson(result, 5000, stoppingToken, "character");
            Logs.Logger.LogInformation("Character Sync Done");
        }

        private static async Task VehicleSyncAsync(CancellationToken stoppingToken)
        {
            Logs.Logger.LogInformation("Syncing Character Vehicles From City");

            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.serviceId}/character_vehicles?select=vehicle_id,owner_cid,model_name,plate,was_boosted&where=was_boosted=0");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            await ProcessLargeJson(result, 5000, stoppingToken, "vehicle");
            Logs.Logger.LogInformation("Vehicle Sync Done");
        }

        private static async Task VehicleFinanceSync(CancellationToken stoppingToken)
        {
            Logs.Logger.LogInformation("Syncing Character Vehicle Finances From City");

            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.serviceId}/finances?select=owner_cid,amount,timestamp,plate,completed");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            try
            {
                await ProcessLargeJson(result, 5000, stoppingToken, "finance");
            }
            catch (Exception ex)
            {
                Logs.Logger.LogError(ex.Message);
            }

            await SyncRepossessedVehicled(result,stoppingToken);
            Logs.Logger.LogInformation("Vehicle Finance Sync Done");
        }

        private static async Task SyncRepossessedVehicled(string finance_list, CancellationToken stopptingToken)
        {
            if (stopptingToken.IsCancellationRequested) return;
            dynamic financed_json = JObject.Parse(finance_list);
            var current_finance_records = new List<string>();
            var records_expired = new List<string>();
            foreach(var v in financed_json["data"])
            {
                current_finance_records.Add(v["plate"].ToString());
            }

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
                var savedFinancesCommand = connection.CreateCommand();
                savedFinancesCommand.CommandType = CommandType.Text;
                savedFinancesCommand.CommandText = "SELECT * FROM character_vehicle_finance;";
                var reader = savedFinancesCommand.ExecuteReader();
                while (reader.Read())
                {
                    var plate = reader["plate"].ToString();
                    if (!current_finance_records.Contains(plate))
                    {
                        records_expired.Add(plate);
                    }
                }
            }

            if(records_expired.Count == 0) return;
            return;
            //This is blocked by chunking missing
            var vehiclesToKeep = await CheckVehiclesRemain(records_expired);


            await using (MySqlConnection connection = new MySqlConnection(connectionString.ToString()))
            {
                connection.Open();
                var inClause = new StringBuilder();
                var keepInClause = new StringBuilder();
                foreach (string plate in records_expired)
                {
                    if(vehiclesToKeep.Contains(plate))
                    {
                        if (keepInClause.Length > 0)
                        {
                            keepInClause.Append(", ");
                        }
                        keepInClause.Append($"'{plate.Replace("'", "''")}'");
                        continue;
                    }

                    if (inClause.Length > 0)
                    {
                        inClause.Append(", ");
                    }
                    inClause.Append($"'{plate.Replace("'", "''")}'"); 
                }
                var command = new StringBuilder();

                if(vehiclesToKeep.Count > 0)
                {
                    command.Append($"UPDATE character_vehicles SET financed = 0 WHERE plate IN ({keepInClause});");
                }

                if(inClause.Length > 0)
                {
                    command.Append($"UPDATE character_vehicles SET repossessed = 1 WHERE plate IN ({inClause}); ");
                    command.Append($"DELETE FROM character_vehicle_finance WHERE  plate in ({inClause});");
                }

                var updateCharacterVehicleCommand = connection.CreateCommand();                
                updateCharacterVehicleCommand.CommandType = CommandType.Text;
                updateCharacterVehicleCommand.CommandText = command.ToString();                    
                updateCharacterVehicleCommand.ExecuteNonQuery();
            }
        }

        public static async Task<List<string>> CheckVehiclesRemain(List<string> vehicles)
        {
            var returnList = new List<string>();

            var query = new StringBuilder();
            //TODO: add chunking logic
            foreach (var vehicle in vehicles)
            {
                if (query.Length > 0)
                {
                    query.Append("|");
                }
                query.Append(vehicle.ToString());
            }

            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.serviceId}/character_vehicles?select=vehicle_id,plate&where=plate={query}");
            request.Headers.Add("X-Version", "1");
            request.Headers.Add("X-Client-Name", "CloudTheWolf.API-Cache-Generator");
            request.Headers.Add("Authorization", "Bearer " + Options.ApiKey);
            var body = await httpClient.SendAsync(request).ConfigureAwait(false);
            var result = await body.Content.ReadAsStringAsync().ConfigureAwait(false);
            var results = JObject.Parse(result);
            if (results.ContainsKey("data"))
            {
                foreach (var vehicle in results["data"])
                {
                    returnList.Add(vehicle["plate"].ToString());
                }
            }
            
            return returnList;
        }

        private static async Task VehicleHoldsSync(CancellationToken stoppingToken)
        {
            var httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(Options.RestUrl);
            var timestamp = DateTimeOffset.Now.ToUnixTimeSeconds();
            Logs.Logger.LogInformation($"Get Holds {timestamp}");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.serviceId}/character_vehicles?select=vehicle_id,owner_cid,model_name,plate,was_boosted,police_impound_expire&where=police_impound_expire>{timestamp}");
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
            Logs.Logger.LogInformation($"Get Inmates {timestamp}");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/{Options.serviceId}/" +
                $"characters?select=character_id,first_name,last_name,jail&where=jail>{timestamp}");
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
            CancellationToken stoppingToken, string type)
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

                var chunkJson = new JObject(new JProperty("data", chunkArray));
                var chunkJsonString = JsonConvert.SerializeObject(chunkJson);

                Console.WriteLine($"Chunk {i}");
                switch (type)
                {
                    case "character":
                        await SyncCharacterToMdt(chunkJsonString);
                        break;
                    case "vehicle":
                        await SyncCharacterVehiclesToMdt(chunkJsonString);
                        break;
                    case "finance":
                        await SyncFianceToMdt(chunkJsonString);
                        break;
                    default:
                        Logs.Logger.LogWarning("{type} is and invalid type", type);
                        break;
                }

                if (stoppingToken.IsCancellationRequested) return;
            }

        }

        private static async Task SyncCharacterToMdt(string input)
        {

            try
            {
                var emojiPattern = @"[\uD800-\uDBFF][\uDC00-\uDFFF]";
                var character_json = Regex.Replace(input, emojiPattern, "?");
                Console.WriteLine($"Sync Start");
                var connectionString = new MySqlConnectionStringBuilder
                {
                    Server = Options.MySqlHost,
                    Port = 3306,
                    UserID = Options.MySqlUsername,
                    Password = Options.MySqlPassword,
                    Database = Options.MySqlDatabase,
                    CharacterSet = "utf8mb4",
                    
                    
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

        private static async Task SyncCharacterVehiclesToMdt(string vehicle_json)
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

                    using (MySqlCommand command = new MySqlCommand("InsertOrUpdateCharacterVehicles", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.Add(new MySqlParameter("vehicle_json", MySqlDbType.LongText)).Value = vehicle_json;

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

        private static async Task SyncFianceToMdt(string finance_json)
        {
            try {
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

                    using (MySqlCommand command = new MySqlCommand("InsertOrUpdateCharacterVehicleFinance", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.Add(new MySqlParameter("finance_json", MySqlDbType.LongText)).Value = finance_json;
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
