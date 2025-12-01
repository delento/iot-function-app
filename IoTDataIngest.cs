using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using MySqlConnector;
using System.Net;

public class IoTDataIngest
{
    private readonly string _connectionString = Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING");

    [Function("IoTDataIngest")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        try
        {
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var envelope = JsonSerializer.Deserialize<IoTEnvelope>(requestBody);

            using var conn = new MySqlConnection(_connectionString);
            await conn.OpenAsync();

            // Example: insert into dailyreading table
            var cmd = new MySqlCommand("INSERT INTO dailyreading (id, DateTime, Readings) VALUES (@id, @dt, @readings)", conn);
            cmd.Parameters.AddWithValue("@id", envelope.Id);
            cmd.Parameters.AddWithValue("@dt", DateTime.UtcNow.AddHours(8)); // convert to UTC+8
            cmd.Parameters.AddWithValue("@readings", JsonSerializer.Serialize(envelope.Data));
            await cmd.ExecuteNonQueryAsync();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteStringAsync("Data inserted successfully");
            return response;
        }
        catch(Exception ex)
        {
            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteStringAsync($"Error: {ex.Message}");
            return response;
        }
    }
}

public class IoTEnvelope
{
    public string Id { get; set; }
    public string Type { get; set; }
    public List<JsonElement> Data { get; set; }
}
