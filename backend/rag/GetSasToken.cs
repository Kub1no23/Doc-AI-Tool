using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using System.Data;
using Microsoft.Data.SqlClient;
using static rag.shared.General;

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

    [Function("GetSasToken")]
    //změna AuthorizationLevel.Function na Anonymous - kvůli přístupu, CORS, v praxi JWT, lepsi nez default key pro delani FE a security
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "documents/token")] HttpRequest req)
    {
        _logger.LogInformation("SAS token request for Blob Storage");

        string? prefix = req.Query["prefix"];
        if (string.IsNullOrWhiteSpace(prefix))
            return new BadRequestObjectResult("Missing prefix query parameter.");
        if (!await PrefixExistsInDatabaseAsync(_sql, prefix))
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
}