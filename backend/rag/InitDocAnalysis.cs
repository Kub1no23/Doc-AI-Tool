using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Net.Http.Headers;
using System.Text.Json;


namespace rag;

public class InitDocAnalysis
{
    private readonly ILogger<InitDocAnalysis> _logger;
    private readonly string _blobConnection;
    private readonly string _sql;
    private readonly string _docIntelligenceEndpoint;
    private readonly string _docIntelligenceKey;

    public InitDocAnalysis(ILogger<InitDocAnalysis> logger)
    {
        _logger = logger;

        _blobConnection = Environment.GetEnvironmentVariable("BlobConnection")
            ?? throw new InvalidOperationException("BlobConnection env variable is missing.");
        _sql = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
        _docIntelligenceEndpoint = Environment.GetEnvironmentVariable("DocIntelligenceEndpoint")
            ?? throw new InvalidOperationException("DocIntelligenceEndpoint env variable is missing.");
        _docIntelligenceKey = Environment.GetEnvironmentVariable("DocIntelligenceKey")
            ?? throw new InvalidOperationException("DocIntelligenceKey env variable is missing.");
    }

    [Function("InitDocAnalysis")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
    {
        _logger.LogInformation("InitializeDocumentAnalysis called");

        string? prefix = req.Query["prefix"];
        if (string.IsNullOrWhiteSpace(prefix))
            return new BadRequestObjectResult("Missing prefix query parameter.");
        if (!PrefixExistsInDatabase(prefix))
            return new BadRequestObjectResult("Invalid prefix.");

        var pdfUrls = await GetPdfUrls(prefix);
        if (pdfUrls.Count == 0)
            return new BadRequestObjectResult("No PDF files uploaded for this prefix.");

        foreach (var url in pdfUrls)
        {
            string operationId = await StartDocumentIntelligenceAnalysis(url);
            SaveDocument(prefix, url, operationId);
        }

        SaveOperationToDatabase(prefix);

        return new OkObjectResult(new
        {
            status = "processing",
            prefix = prefix
        });
    }

    private bool PrefixExistsInDatabase(string prefix)
    {
        using (var conn = new SqlConnection(_sql))
        {
            conn.Open();

            using (var cmd = new SqlCommand(
                "SELECT COUNT(*) FROM analysis WHERE name = @p", conn))
            {
                cmd.Parameters.Add("@p", SqlDbType.NVarChar, 255).Value = prefix;

                int count = (int)cmd.ExecuteScalar();
                return count > 0;
            }
        }
    }

    private async Task<List<string>> GetPdfUrls(string prefix)
    {
        var urls = new List<string>();

        var blobServiceClient = new BlobServiceClient(_blobConnection);
        string containerName = "pdfs";
        var container = blobServiceClient.GetBlobContainerClient(containerName);

        await foreach (var blob in container.GetBlobsAsync(
            traits: BlobTraits.None,
            states: BlobStates.None,
            prefix: $"{prefix}/",
            cancellationToken: default))
        {
            if (blob.Name.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
            {
                urls.Add($"{container.Uri}/{blob.Name}");
            }
        }

        return urls;
    }

    private async Task<string> StartDocumentIntelligenceAnalysis(string pdfUrl)
    {
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", _docIntelligenceKey);

        string fileName = Path.GetFileName(new Uri(pdfUrl).AbsolutePath);

        var blobServiceClient = new BlobServiceClient(_blobConnection);
        var containerClient = blobServiceClient.GetBlobContainerClient("pdfs");
        var blobClient = containerClient.GetBlobClient(fileName);

        string sasUrl = GenerateBlobReadSas(blobClient).ToString();

        var requestBody = new
        {
            urlSource = sasUrl
        };

        var content = new StringContent(JsonSerializer.Serialize(requestBody));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        var reqUrl = $"{_docIntelligenceEndpoint}/formrecognizer/documentModels/prebuilt-layout:analyze?api-version=2023-07-31";

        var response = await http.PostAsync(reqUrl, content);
        string body = await response.Content.ReadAsStringAsync();

        _logger.LogInformation("DI response status: {Status}", response.StatusCode);

        if (!response.IsSuccessStatusCode)
            throw new Exception($"Document Intelligence analyze request failed: {response.StatusCode} - {body}");

        if (!response.Headers.TryGetValues("Operation-Location", out var values))
            throw new Exception("Document Intelligence did not return Operation-Location header.");

        string operationLocation = values.First();
        string operationId = operationLocation
            .Split('/')
            .Last()
            .Split('?')
            .First();

        return operationId;
    }

    private void SaveOperationToDatabase(string prefix)
    {
        using (var conn = new SqlConnection(_sql))
        {
            conn.Open();

            using (var cmd = new SqlCommand(
                "UPDATE analysis SET status = 'processing' WHERE name = @p", conn))
            {
                cmd.Parameters.AddWithValue("@p", prefix);

                cmd.ExecuteNonQuery();
            }
        }
    }

    private void SaveDocument(string prefix, string url, string operationId)
    {
        string fileName = Path.GetFileName(new Uri(url).AbsolutePath);

        using var conn = new SqlConnection(_sql);
        conn.Open();

        using var cmd = new SqlCommand(@"
            INSERT INTO documents (analysis_id, file_name, pdf_url, operation_id, status)
            SELECT id, @fileName, @url, @operationId, 'processing'
            FROM analysis
            WHERE name = @prefix;
        ", conn);

        cmd.Parameters.AddWithValue("@prefix", prefix);
        cmd.Parameters.AddWithValue("@fileName", fileName);
        cmd.Parameters.AddWithValue("@url", url);
        cmd.Parameters.AddWithValue("@operationId", operationId);

        int rows = cmd.ExecuteNonQuery();
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

