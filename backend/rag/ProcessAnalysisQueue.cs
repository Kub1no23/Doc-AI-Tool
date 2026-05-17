using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Azure;
using Azure.AI.OpenAI;
using OpenAI.Chat; // OPRAVA 1: Nová cesta pro OpenAI verze 2.0+

namespace rag
{
    // OPRAVA 2: Třídy od kolegy jsme přidali sem, aby Visual Studio nehlásilo chybějící "rag.shared"
    public enum QueueMessageType { EmbeddingRequest = 1, SimilarityRequest = 2, DocumentAnalysisCompleted = 3, PdfImageExtraction = 4, CleanupTask = 5 }
    public class QueueEnvelope<T> { public QueueMessageType Type { get; set; } public T Payload { get; set; } }
    public class SimilarityReqPayload { public Guid DocumentId { get; set; } }

    // Přepravka pro odpověď od ChatGPT, která přesně odpovídá vaší SQL tabulce
    public class RiskAnalysisResult
    {
        public string Coverage { get; set; }
        public string Explanation { get; set; }
    }

    public class ProcessAnalysisQueue
    {
        private readonly ILogger<ProcessAnalysisQueue> _logger;
        private readonly string _sqlConnection;
        private readonly string _openAiEndpoint;
        private readonly string _openAiKey;

        public ProcessAnalysisQueue(ILogger<ProcessAnalysisQueue> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
            _openAiEndpoint = Environment.GetEnvironmentVariable("OpenAI_Endpoint") ?? throw new Exception("Chybí OpenAI_Endpoint");
            _openAiKey = Environment.GetEnvironmentVariable("OpenAI_ApiKey") ?? throw new Exception("Chybí OpenAI_ApiKey");
        }

        [Function(nameof(ProcessAnalysisQueue))]
        public async Task Run([QueueTrigger("ragqueue", Connection = "AzureWebJobsStorage")] string queueMessage)
        {
            try
            {
                // ROZBALENÍ ZPRÁVY
                string decodedJson = Encoding.UTF8.GetString(Convert.FromBase64String(queueMessage));
                var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
                var envelope = JsonSerializer.Deserialize<QueueEnvelope<JsonElement>>(decodedJson, options);

                if (envelope == null || envelope.Type != QueueMessageType.SimilarityRequest)
                {
                    return; // Ignorujeme, toto zpracovává jiná funkce
                }

                var payload = envelope.Payload.Deserialize<SimilarityReqPayload>(options);
                Guid docId = payload.DocumentId;

                _logger.LogInformation($"Začínám AI analýzu (Reduce 1) pro dokument s ID: {docId}");

                using var conn = new SqlConnection(_sqlConnection);
                await conn.OpenAsync();

                // NAČTENÍ RIZIK
                var risks = new List<(Guid RiskId, string RiskCode, string RiskText, byte[] RiskEmbedding)>();
                string sqlGetRisks = "SELECT id, risk_code, text, embedding FROM risk_vectors";

                using (var cmdRisk = new SqlCommand(sqlGetRisks, conn))
                using (var reader = await cmdRisk.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        risks.Add((
                            reader.GetGuid(0),
                            reader.GetString(1),
                            reader.GetString(2),
                            (byte[])reader["embedding"]
                        ));
                    }
                }

                _logger.LogInformation($"Načetl jsem {risks.Count} rizik k prověření.");

                var openAiClient = new AzureOpenAIClient(new Uri(_openAiEndpoint), new System.ClientModel.ApiKeyCredential(_openAiKey));
                var chatClient = openAiClient.GetChatClient("gpt-4o-mini");

                // PROJDEME KAŽDÉ RIZIKO
                foreach (var risk in risks)
                {
                    _logger.LogInformation($"Hledám riziko {risk.RiskCode} v dokumentu {docId}");

                    string sqlVectorSearch = @"
                        SELECT TOP 3 id, text 
                        FROM pdf_chunks 
                        WHERE document_id = @docId 
                        ORDER BY VECTOR_DISTANCE('cosine', @riskEmbedding, embedding) ASC";

                    var relevantChunks = new List<(Guid ChunkId, string Text)>();

                    using (var cmdSearch = new SqlCommand(sqlVectorSearch, conn))
                    {
                        cmdSearch.Parameters.AddWithValue("@docId", docId);
                        cmdSearch.Parameters.AddWithValue("@riskEmbedding", risk.RiskEmbedding);

                        using (var reader = await cmdSearch.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                relevantChunks.Add((reader.GetGuid(0), reader.GetString(1)));
                            }
                        }
                    }

                    if (relevantChunks.Count == 0) continue;

                    string combinedChunks = string.Join("\n\n--- DALŠÍ ODSTAVEC ---\n\n", relevantChunks.Select(c => c.Text));

                    // OPRAVA 1: Používáme správné třídy z OpenAI.Chat
                    var messages = new List<ChatMessage>
                    {
                        new SystemChatMessage(
                            $"Jsi expertní právní asistent. Tvojí rolí je zhodnotit, zda je v poskytnutém textu smlouvy přítomno riziko typu: '{risk.RiskCode}'.\n" +
                            $"Definice rizika: '{risk.RiskText}'.\n" +
                            "Vyhodnoť pouze texty, které obdržíš. Coverage nastav na 'full' (riziko je tam zcela jasně), 'partial' (jsou tam náznaky, ale není to jednoznačné) nebo 'none' (text o tomto riziku vůbec nepojednává)."
                        ),
                        new UserChatMessage("Zde jsou relevantní odstavce:\n\n" + combinedChunks)
                    };

                    var chatOptions = new ChatCompletionOptions
                    {
                        ResponseFormat = ChatResponseFormat.CreateJsonSchemaFormat(
                            jsonSchemaFormatName: "risk_result_schema",
                            jsonSchema: BinaryData.FromObjectAsJson(new
                            {
                                type = "object",
                                properties = new
                                {
                                    coverage = new { type = "string", @enum = new[] { "full", "partial", "none" } },
                                    explanation = new { type = "string" }
                                },
                                required = new[] { "coverage", "explanation" },
                                additionalProperties = false
                            }),
                            jsonSchemaFormatDescription: null,
                            jsonSchemaIsStrict: true
                        )
                    };

                    var chatResponse = await chatClient.CompleteChatAsync(messages, chatOptions);
                    var aiResult = JsonSerializer.Deserialize<RiskAnalysisResult>(chatResponse.Value.Content[0].Text, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

                    // ULOŽENÍ VÝSLEDKU DO DATABÁZE
                    string chunkIdsJson = JsonSerializer.Serialize(relevantChunks.Select(c => c.ChunkId));

                    string sqlInsert = @"
                        INSERT INTO risk_analysis_results 
                        (document_id, risk_id, coverage, explanation, matched_chunk_ids) 
                        VALUES (@docId, @riskId, @coverage, @explanation, @matchedChunkIds)";

                    using (var cmdInsert = new SqlCommand(sqlInsert, conn))
                    {
                        cmdInsert.Parameters.AddWithValue("@docId", docId);
                        cmdInsert.Parameters.AddWithValue("@riskId", risk.RiskId);
                        cmdInsert.Parameters.AddWithValue("@coverage", aiResult.Coverage);
                        cmdInsert.Parameters.AddWithValue("@explanation", aiResult.Explanation);
                        cmdInsert.Parameters.AddWithValue("@matchedChunkIds", chunkIdsJson);

                        await cmdInsert.ExecuteNonQueryAsync();
                    }
                }

                // DOKONČENÍ ANALÝZY
                string sqlUpdateStatus = "UPDATE documents SET status = 'done' WHERE id = @docId";
                using (var cmdUpdate = new SqlCommand(sqlUpdateStatus, conn))
                {
                    cmdUpdate.Parameters.AddWithValue("@docId", docId);
                    await cmdUpdate.ExecuteNonQueryAsync();
                }

                _logger.LogInformation($"Hotovo! Rizika pro dokument {docId} byla úspěšně vyhodnocena a uložena.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Chyba při vyhodnocování rizik dokumentu: {ex.Message}");
            }
        }
    }
}