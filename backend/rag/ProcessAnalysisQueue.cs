using Azure;
using Azure.AI.OpenAI;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using rag.shared;
using System;
using System.Collections.Generic;
using System.Data; // PŘIDÁNO: Potřebné pro SqlDbType
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace rag
{
    // Přepravka pro odpověď od ChatGPT
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
            _openAiEndpoint = Environment.GetEnvironmentVariable("OpenAI__Endpoint") ?? throw new Exception("Chybí OpenAI__Endpoint");
            _openAiKey = Environment.GetEnvironmentVariable("OpenAI__ApiKey") ?? throw new Exception("Chybí OpenAI__ApiKey");
        }

        [Function(nameof(ProcessAnalysisQueue))]
        public async Task Run([QueueTrigger("ai-analysis-queue", Connection = "AzureWebJobsStorage")] string queueMessage)
        {
            try
            {
                var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
                string jsonToParse = queueMessage;

                // Kontrola Base64
                if (!queueMessage.Trim().StartsWith("{"))
                {
                    jsonToParse = Encoding.UTF8.GetString(Convert.FromBase64String(queueMessage));
                }

                // Přímé dekódování do správného typu
                var envelope = JsonSerializer.Deserialize<QueueEnvelope<SimilarityReqPayload>>(jsonToParse, options);

                if (envelope == null || envelope.Type != QueueMessageType.SimilarityRequest)
                {
                    _logger.LogWarning("Zpráva ignorována: Není typu SimilarityRequest.");
                    return;
                }

                Guid docId = envelope.Payload.DocumentId;
                _logger.LogInformation($"Začínám AI analýzu pro dokument s ID: {docId}");

                using var conn = new SqlConnection(_sqlConnection);
                await conn.OpenAsync();

                // Promazání starých výsledků před novou analýzou
                string sqlClearOld = "DELETE FROM risk_analysis_results WHERE document_id = @docId";
                using (var cmdClear = new SqlCommand(sqlClearOld, conn))
                {
                    cmdClear.Parameters.AddWithValue("@docId", docId);
                    await cmdClear.ExecuteNonQueryAsync();
                }

                // NAČTENÍ RIZIK Z DATABÁZE
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

                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                // PROJDEME KAŽDÉ RIZIKO A POŠLEME HO DO CHATGPT
                foreach (var risk in risks)
                {
                    _logger.LogInformation($"Hledám riziko {risk.RiskCode} v dokumentu {docId}");

                    // 1. Z přečtených UTF-8 bajtů uděláme zpátky čistý JSON string
                    string jsonVector = Encoding.UTF8.GetString(risk.RiskEmbedding);

                    // OPRAVA 2: Tady je ten zázračný trojitý CAST! 
                    // Ty JSON bajty, co jsme si tam uložili, si tu převedeme zpátky na VARCHAR(MAX) 
                    // a následně na nativní typ VECTOR. Azure SQL to bez remcání sežere.
                    // OPRAVA 2: Čistý SQL dotaz
                    // 2. ZMĚNA: SQL dotaz s dvojitým jištěním
                    // @riskEmbedding se teď předává jako obyčejný VARCHAR a převede se nativně (protože to SQL z textu umí).
                    // embedding sloupec se musí "oříznout" a pak převést na VECTOR.
                    // 2. TOTO JE TA OPRAVA: Místo nefunkčního SUBSTRING použijeme tvrdý CAST na VARBINARY(8000), 
                    // čímž sloupci 'embedding' definitivně sebereme přívlastek (MAX) a pak z něj teprve uděláme VECTOR.
                    // 2. V SQL použijeme CAST z textu (VARCHAR) na VECTOR. Tohle je oficiálně podporovaná cesta!
                    string sqlVectorSearch = @"
                        SELECT TOP 3 id, text 
                        FROM pdf_chunks 
                        WHERE document_id = @docId 
                        ORDER BY VECTOR_DISTANCE(
                            'cosine', 
                            CAST(@riskEmbedding AS VECTOR(1536)), 
                            CAST(CAST(embedding AS VARCHAR(MAX)) AS VECTOR(1536))
                        ) ASC";

                    var relevantChunks = new List<(Guid ChunkId, string Text)>();

                    using (var cmdSearch = new SqlCommand(sqlVectorSearch, conn))
                    {
                        cmdSearch.Parameters.AddWithValue("@docId", docId);

                        // Posíláme do SQL čistý text (NVARCHAR). SQL Server zajásá.
                        cmdSearch.Parameters.AddWithValue("@riskEmbedding", jsonVector);

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