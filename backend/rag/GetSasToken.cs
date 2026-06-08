using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using System.Data;
using Microsoft.Data.SqlClient;

namespace rag;

public class GetSasToken
{
    private readonly ILogger<GetSasToken> _logger;
    private readonly string _connectionString;
    private readonly string _sql;

    public GetSasToken(ILogger<GetSasToken> logger)
    {
        _logger = logger;
        _connectionString = Environment.GetEnvironmentVariable("BlobConnection")
            ?? throw new InvalidOperationException("BlobConnection env variable is missing.");
        _sql = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
    }

    private string GetAccountKeyFromConnectionString()
    {
        var parts = _connectionString.Split(';');

        foreach (var part in parts)
        {
            if (part.StartsWith("AccountKey=", StringComparison.OrdinalIgnoreCase))
            {
                return part.Substring("AccountKey=".Length);
            }
        }

        throw new Exception("AccountKey not found in connection string.");
    }
    private async Task<bool> PrefixExistsInDatabaseAsync(string prefix)
    {
        using (var conn = new SqlConnection(_sql))
        {
            await conn.OpenAsync();

            using (var cmd = new SqlCommand("SELECT COUNT(*) FROM analysis WHERE name = @p", conn))
            {
                cmd.Parameters.AddWithValue("@p", prefix);

                int count = (int)await cmd.ExecuteScalarAsync();
                return count > 0;
            }
        }
    }

    [Function("GetSasToken")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        _logger.LogInformation("SAS token request for Blob Storage");

        string? prefix = req.Query["prefix"];
        if (string.IsNullOrWhiteSpace(prefix))
            return new BadRequestObjectResult("Missing prefix query parameter.");
        if (!await PrefixExistsInDatabaseAsync(prefix))
            return new BadRequestObjectResult("Invalid prefix.");

        string? mode = req.Query["mode"];
        var permissions = BlobContainerSasPermissions.Read;
        if (mode?.ToLower() == "upload")
        {
            permissions |= BlobContainerSasPermissions.Add |
                           BlobContainerSasPermissions.Create |
                           BlobContainerSasPermissions.Write |
                           BlobContainerSasPermissions.List;
        }

        var blobServiceClient = new BlobServiceClient(_connectionString);
        string containerName = "pdfs";
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

        var sasBuilder = new BlobSasBuilder(permissions, DateTimeOffset.UtcNow.AddMinutes(30))
        {
            BlobContainerName = containerName,
            Resource = "c"
        };

        var sasToken = sasBuilder.ToSasQueryParameters(
            new StorageSharedKeyCredential(
                blobServiceClient.AccountName,
                GetAccountKeyFromConnectionString()
            )
        ).ToString();

        return new OkObjectResult(new
        {
            containerUrl = containerClient.Uri.ToString(),
            sasToken,
            prefix,
            expires = sasBuilder.ExpiresOn
        });
    }
}