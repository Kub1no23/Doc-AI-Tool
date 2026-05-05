using Grpc.Core;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using rag.shared;
using System.Text.Json;

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

        var baseUrl = _Endpoint.TrimEnd('/');
        var url = $"{baseUrl}/documentintelligence/documentModels/prebuilt-layout/analyzeResults/{OperationId}?api-version=2024-11-30";

        _logger.LogInformation("Polling URL: {0}", url);

        var response = await http.GetAsync(url);
        string json = await response.Content.ReadAsStringAsync();

        _logger.LogInformation("DI HTTP status: {0}", response.StatusCode);
        _logger.LogInformation("DI RAW JSON length: {0} chars", json.Length);

        string status = "unknown";

        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("status", out var statusProp))
            {
                status = statusProp.GetString()?.ToLowerInvariant() ?? "unknown";
                _logger.LogInformation("Extracted status: {0}", status);
            }
            else if (root.TryGetProperty("error", out var err))
            {
                _logger.LogError("DI ERROR OBJECT: {0}", err.ToString());
                status = "failed";
            }
            else
            {
                _logger.LogWarning("No 'status' or 'error' field found in DI response.");

                if (!response.IsSuccessStatusCode)
                {
                    status = "failed";
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("JSON parse error: {0}", ex.Message);
            status = !response.IsSuccessStatusCode ? "failed" : "unknown";
        }

        _logger.LogInformation("Final parsed status: {0}", status);
        _logger.LogInformation("=== FetchOperationAsync END ===");

        return (status, json);
    }
}
public class CheckDocAnalysis
{
    private readonly ILogger<CheckDocAnalysis> _logger;
    private readonly string _sql;
    private readonly string _docIntelligenceEndpoint;
    private readonly string _docIntelligenceKey;

    public CheckDocAnalysis(ILogger<CheckDocAnalysis> logger)
    {
        _logger = logger;

        _sql = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
        _docIntelligenceEndpoint = Environment.GetEnvironmentVariable("DocIntelligenceEndpoint")
            ?? throw new InvalidOperationException("DocIntelligenceEndpoint env variable is missing.");
        _docIntelligenceKey = Environment.GetEnvironmentVariable("DocIntelligenceKey")
            ?? throw new InvalidOperationException("DocIntelligenceKey env variable is missing.");
    }

    [Function("CheckDocAnalysis")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        string? prefix = req.Query["prefix"];
        if (string.IsNullOrWhiteSpace(prefix))
            return new BadRequestObjectResult("Missing prefix query parameter.");
        if (!PrefixExistsInDatabase(prefix))
            return new BadRequestObjectResult("Invalid prefix.");

        var docs = GetProcessingDocuments(prefix);
        if (docs.Count == 0)
        {
            return new BadRequestObjectResult(new
            {
                message = "No documents are currently processing for this analysis.",
                prefix
            });
        }

        // FÁZE 1 — stáhnout výsledky všech dokumentů
        var results = new List<(DocInfo Doc, string Status, string Json)>();

        foreach (var doc in docs)
        {
            var (status, json) = await doc.FetchOperationAsync();
            results.Add((doc, status, json));
        }

        // FÁZE 2 — vyhodnocení

        // 1) Některý dokument stále běží
        if (results.Any(r => r.Status is "notStarted" or "running"))
        {
            return new OkObjectResult(new
            {
                status = "processing",
                prefix,
                documents = results.Select(r => new
                {
                    file = r.Doc.FileName,
                    status = r.Status
                })
            });
        }

        // 2) Některý dokument failnul
        var failed = results.FirstOrDefault(r => r.Status is "failed" or "unknown");
        if (failed.Doc != null)
        {
            UpdateDocumentStatus(failed.Doc.Id, "error");

            return new OkObjectResult(new
            {
                status = "failed",
                prefix,
                documents = results.Select(r => new
                {
                    file = r.Doc.FileName,
                    status = r.Status
                })
            });
        }

        // 3) Všechny dokumenty jsou hotové → uložit výsledky
        foreach (var r in results)
        {
            SaveDocumentResult(prefix, r.Doc.FileName, r.Json); // for testing
            var payload = new EmbedReqPayload
            {
                DocumentId = r.Doc.Id,
                DiResult = JsonDocument.Parse(r.Json).RootElement.Clone()
            };
            await QueueSender.SendToQueueAsync(QueueMessageType.EmbeddingRequest, payload);
            UpdateDocumentStatus(r.Doc.Id, "done");
        }

        MarkAnalysisDone(prefix);

        return new OkObjectResult(new
        {
            status = "done",
            prefix,
            documents = results.Select(r => new
            {
                file = r.Doc.FileName,
                status = r.Status
            })
        });
    }
    private bool PrefixExistsInDatabase(string prefix)
    {
        using (var conn = new SqlConnection(_sql))
        {
            conn.Open();

            using (var cmd = new SqlCommand("SELECT COUNT(*) FROM analysis WHERE name = @p", conn))
            {
                cmd.Parameters.AddWithValue("@p", prefix);
                int count = (int)cmd.ExecuteScalar();
                return count > 0;
            }
        }
    }
    private List<DocInfo> GetProcessingDocuments(string prefix)
    {
        var list = new List<DocInfo>();

        using var conn = new SqlConnection(_sql);
        conn.Open();

        using var cmd = new SqlCommand(@"
            SELECT d.id, d.file_name, d.operation_id, d.status
            FROM documents d
            JOIN analysis a ON d.analysis_id = a.id
            WHERE a.name = @p AND d.status = 'processing';
        ", conn);

        cmd.Parameters.AddWithValue("@p", prefix);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
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
    private void SaveDocumentResult(string prefix, string fileName, string json)
    {
        string folder = Path.Combine(Environment.CurrentDirectory, "analysis-results", prefix);
        Directory.CreateDirectory(folder);

        string filePath = Path.Combine(folder, $"{fileName}.json");
        File.WriteAllText(filePath, json);
    }

    private void UpdateDocumentStatus(Guid id, string status)
    {
        using var conn = new SqlConnection(_sql);
        conn.Open();

        using var cmd = new SqlCommand(
            "UPDATE documents SET status = @status WHERE id = @id", conn);

        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@status", status);

        cmd.ExecuteNonQuery();
    }

    private void MarkAnalysisDone(string prefix)
    {
        using var conn = new SqlConnection(_sql);
        conn.Open();

        using var cmd = new SqlCommand(
            "UPDATE analysis SET status = 'done' WHERE name = @p", conn);

        cmd.Parameters.AddWithValue("@p", prefix);
        cmd.ExecuteNonQuery();
    }

}

