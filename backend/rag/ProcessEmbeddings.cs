using Azure.AI.OpenAI; // Tady už to správně máš
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI;
using OpenAI.Embeddings;
using rag;
using rag.shared;
using System;
using System.ClientModel;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

public class EmbeddingService
{
    private readonly EmbeddingClient _embeddingsClient;

    public EmbeddingService(string endpoint, string apiKey, string model)
    {
        // OPRAVENO: Používáme AzureOpenAIClient místo obyčejného OpenAIClient
        AzureOpenAIClient client = new(new Uri(endpoint), new ApiKeyCredential(apiKey));

        EmbeddingClient embeddingClient = client.GetEmbeddingClient(model);
        _embeddingsClient = embeddingClient;
    }

    public async Task<OpenAIEmbeddingCollection> CreateEmbeddingAsync(string[] sArr)
    {
        // OPRAVA TADY: Vynutíme maximální velikost 1536, abychom nenarazili do limitu Azure SQL!
        var options = new EmbeddingGenerationOptions { Dimensions = 1536 };
        var response = await _embeddingsClient.GenerateEmbeddingsAsync(sArr, options);
        return response.Value;
    }
}

public record ChunkInfo(int PageNumber, int ChunkIndex, string Text);

public class ProcessEmbeddings
{
    private readonly ILogger _logger;
    private readonly EmbeddingService _embeddingService;
    private readonly string _sqlConnectionString;

    public ProcessEmbeddings(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ProcessEmbeddings>();
        var endpoint = Environment.GetEnvironmentVariable("OpenAI__Endpoint")
            ?? throw new InvalidOperationException("Missing environment variable: OpenAI__Endpoint");
        var key = Environment.GetEnvironmentVariable("OpenAI__ApiKey")
            ?? throw new InvalidOperationException("Missing environment variable: OpenAI__ApiKey");
        var model = Environment.GetEnvironmentVariable("EmbeddingModel")
            ?? throw new InvalidOperationException("Missing environment variable: EmbeddingModel");
        _sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("Missing environment variable: SqlConnection");

        _embeddingService = new EmbeddingService(endpoint, key, model);

        _logger.LogInformation("Endpoint: {e}, Model: {m}", endpoint, model);
    }

    [Function("ProcessEmbeddings")]
    public async Task Run([QueueTrigger("pdf-embedding-queue", Connection = "AzureWebJobsStorage")] string message)
    {
        _logger.LogInformation("Received queue message");

        var envelope = QueueMessageHelper.Deserialize<QueueEnvelope<EmbedReqPayload>>(message);

        if (envelope == null)
        {
            _logger.LogError("Failed to deserialize envelope");
            return;
        }
        if (envelope.Type != QueueMessageType.EmbeddingRequest)
        {
            _logger.LogWarning("Ignoring message of type {Type}", envelope.Type);
            return;
        }

        var payload = envelope.Payload;

     

        // ---------------------------------------------------------
        // CLOUD CLAIM CHECK: Vyzvednutí výsledků z Azure Blob Storage
        // ---------------------------------------------------------
        string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("ocr-results");

        string blobName = $"{payload.Prefix}/{payload.FileName}.json";
        BlobClient blobClient = containerClient.GetBlobClient(blobName);

        if (!await blobClient.ExistsAsync())
        {
            _logger.LogError($"JSON data chybí v Blobu! Hledáno: {blobName}");
            return;
        }

        _logger.LogInformation($"Čtu JSON z Blobu: {blobName}");
        var response = await blobClient.DownloadContentAsync();
        string jsonContent = response.Value.Content.ToString();

        using var jsonDoc = JsonDocument.Parse(jsonContent);
        var analyzeResult = jsonDoc.RootElement.GetProperty("analyzeResult");
        // ---------------------------------------------------------

        var chunks = ChunkByPages(analyzeResult, 800); // 800 chars per chunk
        _logger.LogInformation("Chunked into {Count} chunks", chunks.Count);

        var textChunks = chunks.Select(c => c.Text).ToArray();

        var embeddingCollection = await _embeddingService.CreateEmbeddingAsync(textChunks);
        var items = embeddingCollection.ToList();

        // Promazání starých kousků před vložením nových (prevence duplicit)
        await ClearOldChunksAsync(payload.DocumentId);

        for (int i = 0; i < chunks.Count; i++)
        {
            float[] vFloats = items[i].ToFloats().ToArray();

            await SaveChunkToDbAsync(
                payload.DocumentId,
                chunks[i].PageNumber,
                chunks[i].ChunkIndex,
                chunks[i].Text,
                vFloats
            );

            _logger.LogInformation("Chunk saved. Text length: {len}", chunks[i].Text.Length);
        }

        // Předání štafety dál pro tvou AI (Pás 2)
        await QueueSender.SendToQueueAsync(QueueMessageType.SimilarityRequest, new SimilarityReqPayload
        {
            DocumentId = payload.DocumentId,
        });
    }

    private List<ChunkInfo> ChunkByPages(JsonElement analyzeResult, int chunkSize)
    {
        var result = new List<ChunkInfo>();
        var pages = analyzeResult.GetProperty("pages").EnumerateArray();

        foreach (var page in pages)
        {
            int pageNumber = page.GetProperty("pageNumber").GetInt32();
            string pageText = ExtractPageText(page);

            var chunks = ChunkText(pageText, chunkSize);

            for (int i = 0; i < chunks.Length; i++)
            {
                result.Add(new ChunkInfo(
                    PageNumber: pageNumber,
                    ChunkIndex: i,
                    Text: chunks[i]
                ));
            }
        }

        return result;
    }

    private string ExtractPageText(JsonElement page)
    {
        var lines = page.GetProperty("lines").EnumerateArray();
        var sb = new StringBuilder();

        foreach (var line in lines)
        {
            string text = line.GetProperty("content").GetString() ?? "";
            sb.AppendLine(text);
        }

        return sb.ToString().Trim();
    }

    private string[] ChunkText(string text, int maxLength)
    {
        var chunks = new List<string>();
        int index = 0;

        while (index < text.Length)
        {
            int length = Math.Min(maxLength, text.Length - index);
            chunks.Add(text.Substring(index, length));
            index += length;
        }

        return chunks.ToArray();
    }

    private async Task ClearOldChunksAsync(Guid documentId)
    {
        using var conn = new SqlConnection(_sqlConnectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM pdf_chunks WHERE document_id = @document_id";
        cmd.Parameters.AddWithValue("@document_id", documentId);

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SaveChunkToDbAsync(Guid documentId, int pageNumber, int chunkIndex, string text, float[] embedding)
    {
        using var conn = new SqlConnection(_sqlConnectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO pdf_chunks (document_id, page_number, chunk_index, text, embedding)
            VALUES (@document_id, @page_number, @chunk_index, @text, @embedding)
        ";

        cmd.Parameters.AddWithValue("@document_id", documentId);
        cmd.Parameters.AddWithValue("@page_number", pageNumber);
        cmd.Parameters.AddWithValue("@chunk_index", chunkIndex);
        cmd.Parameters.AddWithValue("@text", text);

        // OPRAVA: I odstavce smlouvy uložíme jako JSON text převedený na UTF-8 bajty
        string jsonEmbedding = JsonSerializer.Serialize(embedding);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(jsonEmbedding);
        cmd.Parameters.AddWithValue("@embedding", jsonBytes);

        await cmd.ExecuteNonQueryAsync();
    }
}