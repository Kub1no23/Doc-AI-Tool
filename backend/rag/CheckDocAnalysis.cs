using Azure.Storage.Blobs;
using Grpc.Core;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using rag.shared;
using static rag.shared.General;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

internal class DocInfo
{
    public Guid Id { get; }
    public string FileName { get; }
    public string OperationId { get; }
    public string Status { get; set; }
    public string? RawJson { get; set; }

    private readonly string _Endpoint;
    private readonly string _Key;
    private readonly ILogger<CheckDocAnalysis> _logger;

    public DocInfo(Guid id, string fileName, string operationId, string status, string endpoint, string key, ILogger<CheckDocAnalysis> logger)
    {
        Id = id;
        FileName = fileName;
        OperationId = operationId;
        Status = status;
        _Endpoint = endpoint;
        _Key = key;
        _logger = logger;
    }

    public async Task<(string Status, string RawJson)> FetchOperationAsync()
    {
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", _Key);

        var url = $"{_Endpoint}/documentintelligence/documentModels/prebuilt-layout/analyzeResults/{OperationId}?api-version=2024-11-30";
        var response = await http.GetAsync(url);

        _logger.LogInformation("DI HTTP status: {0}", response.StatusCode);

        string json = await response.Content.ReadAsStringAsync();
        _logger.LogInformation("DI raw JSON: {0}", json);


        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        string status = "unknown";
        if (root.TryGetProperty("status", out var statusProp))
        {
            status = statusProp.GetString() ?? "unknown";
        }
        if (status == "failed" && root.TryGetProperty("error", out var err))
        {
            _logger.LogError("Document Intelligence analysis FAILED. Reason: {0}", err.ToString());
        }

        return (status, json);
    }
}

public class CheckDocAnalysis
{
    private readonly ILogger<CheckDocAnalysis> _logger;
    private readonly string _sql;
    private readonly string _blobConnection;
    private readonly string _docIntelligenceEndpoint;
    private readonly string _docIntelligenceKey;

    public CheckDocAnalysis(ILogger<CheckDocAnalysis> logger)
    {
        _logger = logger;

        _sql = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
        _blobConnection = Environment.GetEnvironmentVariable("BlobConnection")
            ?? throw new InvalidOperationException("BlobConnection env variable is missing.");
        _docIntelligenceEndpoint = Environment.GetEnvironmentVariable("DocIntelligenceEndpoint")
            ?? throw new InvalidOperationException("DocIntelligenceEndpoint env variable is missing.");
        _docIntelligenceKey = Environment.GetEnvironmentVariable("DocIntelligenceKey")
            ?? throw new InvalidOperationException("DocIntelligenceKey env variable is missing.");
    }

    [Function("CheckDocAnalysis")]
  
    public async Task Run([QueueTrigger("pdf-json-queue", Connection = "MyDataStorage")] QueueEnvelope<DocAIReqPayload> message)
    {
        _logger.LogInformation("CheckDocumentAnalysis called");

        //// 1. Dekódování z Base64 na čistý JSON
        //string jsonToParse = queueMessage;
        //if (!queueMessage.Trim().StartsWith("{"))
        //{
        //    jsonToParse = Encoding.UTF8.GetString(Convert.FromBase64String(queueMessage));
        //}

        //// 2. Deserializace z JSONu do objektu
        //var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        //var message = JsonSerializer.Deserialize<QueueEnvelope<DocAIReqPayload>>(jsonToParse, options);

        //if (message == null || string.IsNullOrWhiteSpace(message.Payload.Prefix))
        //{
        //    _logger.LogWarning("Queue returned a prefix that is null. Ignoring.");
        //    return;
        //}


        string prefix = message.Payload.Prefix;



        if (!await PrefixExistsInDatabaseAsync(_sql, prefix))
        {
            _logger.LogError($"Queue returned an invalid prefix {prefix} that doesn't exist in DB. Ignoring.");
            return;
        }

        var ocrDocs = await GetAllDocumentsAsync(prefix);
        if (ocrDocs.Count == 0)
        {
            _logger.LogWarning($"No documents for processing found in DB for prefix {prefix}. Ignoring.");
            return;
        }

        foreach (var doc in ocrDocs)
        {
            var (diStatus, json) = await doc.FetchOperationAsync();

            if (diStatus is "failed" or "unknown")
            {
                doc.Status = "error";
                await UpdateDocumentStatusAsync(_sql, doc.Id, doc.Status);
                await UpdateAnalysisStatusAsync(_sql, prefix, "error");
            }
            else if (diStatus is "running" or "notStarted")
            {
                await QueueSender.SendToQueueAsync(QueueMessageType.DocAIRequest, new DocAIReqPayload { Prefix = prefix }, delaySec: 100);
            }
            else if (diStatus == "succeeded")
            {
                await SaveDocumentResultAsync(doc.FileName, json);

                doc.Status = "processing_ai";
                await UpdateDocumentStatusAsync(_sql, doc.Id, doc.Status);

                await QueueSender.SendToQueueAsync(QueueMessageType.EmbeddingRequest, new EmbedReqPayload
                {
                    DocumentId = doc.Id,
                    Prefix = prefix,
                    FileName = doc.FileName
                });
            }
        }
    }

    private async Task<List<DocInfo>> GetAllDocumentsAsync(string prefix)
    {
        var list = new List<DocInfo>();

        using var conn = new SqlConnection(_sql);
        await conn.OpenAsync();

        using var cmd = new SqlCommand(@"
            SELECT d.id, d.file_name, d.operation_id, d.status
            FROM documents d
            JOIN analysis a ON d.analysis_id = a.id
            WHERE a.name = @p AND d.status = 'processing';
        ", conn);

        cmd.Parameters.AddWithValue("@p", prefix);

        using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            list.Add(new DocInfo(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                _docIntelligenceEndpoint,
                _docIntelligenceKey,
                _logger
            ));
        }

        return list;
    }

    private async Task SaveDocumentResultAsync(string fileName, string json)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(_blobConnection);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("ocr-results");
        await containerClient.CreateIfNotExistsAsync();

        string blobName = $"{fileName}.json";
        BlobClient blobClient = containerClient.GetBlobClient(blobName);

        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
        {
            await blobClient.UploadAsync(stream, overwrite: true);
        }
    }

}