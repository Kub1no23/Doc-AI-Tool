using Azure;
using Azure.AI.DocumentIntelligence;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using rag.shared;
using static rag.shared.General;

namespace rag;

public class InitDocAnalysis
{
    private readonly ILogger<InitDocAnalysis> _logger;
    private readonly string _blobConnection;
    private readonly string _sql;
    private readonly DocumentIntelligenceClient _docClient;

    public InitDocAnalysis(ILogger<InitDocAnalysis> logger)
    {
        _logger = logger;

        _blobConnection = Environment.GetEnvironmentVariable("BlobConnection")
            ?? throw new InvalidOperationException("BlobConnection env variable is missing.");
        _sql = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
        var endpoint = Environment.GetEnvironmentVariable("DocIntelligenceEndpoint")
            ?? throw new InvalidOperationException("DocIntelligenceEndpoint env variable is missing.");
        var apiKey = Environment.GetEnvironmentVariable("DocIntelligenceKey")
            ?? throw new InvalidOperationException("DocIntelligenceKey env variable is missing.");

        var options = new DocumentIntelligenceClientOptions(DocumentIntelligenceClientOptions.ServiceVersion.V2024_11_30);
        _docClient = new DocumentIntelligenceClient(new Uri(endpoint), new AzureKeyCredential(apiKey), options);
        _logger.LogInformation($"Spouštím analýzu pomocí modelu API 2024-11-30...");
    }

    [Function("InitDocAnalysis")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "documents/analyze")] HttpRequest req)
    {
        _logger.LogInformation("InitializeDocumentAnalysis called");

        string? prefix = req.Query["prefix"];
        if (string.IsNullOrWhiteSpace(prefix))
            return new BadRequestObjectResult("Missing prefix query parameter.");
        if (!await PrefixExistsInDatabaseAsync(_sql, prefix))
            return new BadRequestObjectResult("Invalid prefix.");

        var blobNames = await GetBlobNames(prefix);
        if (blobNames.Count == 0)
            return new BadRequestObjectResult("No PDF files uploaded for this prefix.");

        foreach (var bN in blobNames)
        {
            string operationId = await StartDocumentIntelligenceAnalysis(bN);
            await SaveDocumentAsync(prefix, bN, operationId);
        }

        await SaveOperationToDatabaseAsync(prefix);

        await QueueSender.SendToQueueAsync(QueueMessageType.DocAIRequest, new DocAIReqPayload { Prefix = prefix }, delaySec: 100);

        return new OkObjectResult(new
        {
            status = "processing",
            prefix = prefix
        });
    }


    private async Task<List<string>> GetBlobNames(string prefix)
    {
        var blobNames = new List<string>();

        var blobServiceClient = new BlobServiceClient(_blobConnection);
        string containerName = "pdfs";
        var container = blobServiceClient.GetBlobContainerClient(containerName);

        await foreach (var blob in container.GetBlobsAsync(
            traits: BlobTraits.None,
            states: BlobStates.None,
            prefix: $"{prefix}/",
            cancellationToken: default))
        {
            if (blob.Name.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase) ||
                blob.Name.EndsWith(".png", StringComparison.OrdinalIgnoreCase) ||
                blob.Name.EndsWith(".jpg", StringComparison.OrdinalIgnoreCase) ||
                blob.Name.EndsWith(".jpeg", StringComparison.OrdinalIgnoreCase))
            {
                blobNames.Add(blob.Name);
            }
        }

        return blobNames;
    }

    private async Task<string> StartDocumentIntelligenceAnalysis(string blobName)
    {
        var blobServiceClient = new BlobServiceClient(_blobConnection);
        var containerClient = blobServiceClient.GetBlobContainerClient("pdfs");
        var blobClient = containerClient.GetBlobClient(blobName);

        string sasUrl = GenerateBlobReadSas(blobClient).ToString();

        _logger.LogInformation("DI REQUEST START: model=prebuilt-layout, urlSource={Url}", sasUrl);

        try
        {
            Operation operation = await _docClient.AnalyzeDocumentAsync(WaitUntil.Started, "prebuilt-layout", new Uri(sasUrl));

            string operationId = operation.Id;

            _logger.LogInformation("DI RESPONSE SUCCESS: Operation-Location header extracted. OperationId: {OperationId}", operationId);

            return operationId;
        }
        catch (RequestFailedException ex)
        {
            _logger.LogError(ex, "DI RESPONSE ERROR: Status {Status}. Body: {Body}", ex.Status, ex.Message);
            throw new Exception($"Document Intelligence analyze request failed: {ex.Status} - {ex.Message}", ex);
        }
    }

    private async Task SaveOperationToDatabaseAsync(string prefix)
    {
        using (var conn = new SqlConnection(_sql))
        {
            await conn.OpenAsync();

            using (var cmd = new SqlCommand("UPDATE analysis SET status = 'processing' WHERE name = @p", conn))
            {
                cmd.Parameters.AddWithValue("@p", prefix);

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    private async Task SaveDocumentAsync(string prefix, string blobName, string operationId)
    {
        using var conn = new SqlConnection(_sql);

        await conn.OpenAsync();

        using var cmd = new SqlCommand(@"
            INSERT INTO documents (analysis_id, file_name, operation_id, status)
            SELECT id, @fileName, @operationId, 'processing'
            FROM analysis
            WHERE name = @prefix;
        ", conn);

        cmd.Parameters.AddWithValue("@prefix", prefix);
        cmd.Parameters.AddWithValue("@fileName", blobName);
        cmd.Parameters.AddWithValue("@operationId", operationId);

        int rows = await cmd.ExecuteNonQueryAsync();

        if (rows == 0)
            throw new Exception($"Analysis '{prefix}' not found.");
    }

    private Uri GenerateBlobReadSas(BlobClient blobClient)
    {
        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = blobClient.BlobContainerName,
            BlobName = blobClient.Name,
            Resource = "b",
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(30)
        };

        sasBuilder.SetPermissions(BlobSasPermissions.Read);

        return blobClient.GenerateSasUri(sasBuilder);
    }
}