using Azure;
using Azure.AI.OpenAI;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using rag.shared;
using System;
using System.Collections.Generic;
using System.Data;
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
        public async Task Run([QueueTrigger("ai-analysis-queue", Connection = "MyDataStorage")] string queueMessage)
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

                string sqlClearOld = "DELETE FROM risk_analysis_results WHERE document_id = @docId";
                using (var cmdClear = new SqlCommand(sqlClearOld, conn))
                {
                    cmdClear.Parameters.AddWithValue("@docId", docId);
                    await cmdClear.ExecuteNonQueryAsync();
                }

                // VYNULOVÁNÍ SKÓRE před novou analýzou
                string sqlResetScore = "UPDATE documents SET total_risk_score = 0 WHERE id = @docId";
                using (var cmdReset = new SqlCommand(sqlResetScore, conn))
                {
                    cmdReset.Parameters.AddWithValue("@docId", docId);
                    await cmdReset.ExecuteNonQueryAsync();
                }

                // Přidáno načtení i risk_weight z databáze, abychom to mohli použít při výpočtu
                var risks = new List<(Guid RiskId, string RiskCode, string RiskText, int RiskWeight, byte[] RiskEmbedding)>();
                string sqlGetRisks = "SELECT id, risk_code, text, risk_weight, embedding FROM risk_vectors";

                using (var cmdRisk = new SqlCommand(sqlGetRisks, conn))
                using (var reader = await cmdRisk.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        risks.Add((
                            reader.GetGuid(0),
                            reader.GetString(1),
                            reader.GetString(2),
                            reader.GetInt32(3), // Načtení risk_weight
                            (byte[])reader["embedding"]
                        ));
                    }
                }

                _logger.LogInformation($"Načetl jsem {risks.Count} rizik k prověření.");

                var openAiClient = new AzureOpenAIClient(new Uri(_openAiEndpoint), new System.ClientModel.ApiKeyCredential(_openAiKey));
                var chatClient = openAiClient.GetChatClient("gpt-4o-mini");

                float calculatedTotalScore = 0.0f;

                foreach (var risk in risks)
                {
                    _logger.LogInformation($"Hledám riziko {risk.RiskCode} v dokumentu {docId}");

                    string jsonVector = Encoding.UTF8.GetString(risk.RiskEmbedding);

                    string sqlVectorSearch = @"
                        SELECT TOP 3 id, text, page_number 
                        FROM pdf_chunks 
                        WHERE document_id = @docId 
                        ORDER BY VECTOR_DISTANCE(
                            'cosine', 
                            CAST(@riskEmbedding AS VECTOR(1536)), 
                            CAST(CAST(embedding AS VARCHAR(MAX)) AS VECTOR(1536))
                        ) ASC";

                    var relevantChunks = new List<(Guid ChunkId, string Text, int PageNumber)>();

                    using (var cmdSearch = new SqlCommand(sqlVectorSearch, conn))
                    {
                        cmdSearch.Parameters.AddWithValue("@docId", docId);
                        cmdSearch.Parameters.AddWithValue("@riskEmbedding", jsonVector);

                        using (var reader = await cmdSearch.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                relevantChunks.Add((
                                    reader.GetGuid(0),
                                    reader.GetString(1),
                                    reader.GetInt32(2)
                                ));
                            }
                        }
                    }
                    if (relevantChunks.Count == 0) continue;

                    string combinedChunks = string.Join("\n\n--- DALŠÍ ODSTAVEC ---\n\n",
                        relevantChunks.Select(c => $"[STRANA {c.PageNumber}]:\n{c.Text}"));

                    string systemPrompt = $@"
Jsi expertní právní asistent. Tvojí rolí je zhodnotit, zda je v poskytnutém textu smlouvy přítomno riziko typu: '{risk.RiskCode}'.
Definice rizika: '{risk.RiskText}'.

POZOR NA NEGACE A VYLOUČENÍ RIZIKA (Kritické pravidlo):
Často se stává, že smlouva o daném tématu mluví, ale výslovně ho vylučuje. 
Pokud text uvádí, že někdo 'nenese odpovědnost', 'riziko je vyloučeno', 'náklady nese druhá strana', 'neodpovídá' nebo 'nemá vliv', ZNAMENÁ TO, ŽE RIZIKO NENÍ PŘÍTOMNO. V takovém případě MUSÍŠ striktně nastavit coverage na 'none'. Nesmíš text označit jako rizikový jen proto, že obsahuje klíčová slova.

PRAVIDLA PRO HODNOCENÍ (COVERAGE):
- 'full': Riziko je v textu jasně a platně uplatněno (někdo ho reálně nese).
- 'partial': V textu jsou náznaky nebo podmínky, ale uplatnění není stoprocentně jednoznačné.
- 'none': Text o riziku buď vůbec nepojednává, NEBO ho výslovně vylučuje/negate.

DŮLEŽITÉ PRAVIDLO PRO TVOJI ODPOVĚĎ A CITACE (EXPLANATION):
Každý odstavec, který obdržíš, začíná informací o tom, na jaké je straně (např. [STRANA 5]:).
Pokud je coverage 'full' nebo 'partial', MUSÍŠ do svého vysvětlení přidat krátkou citaci z textu a za ni uvést značku [[page:X]], kde X je číslo příslušné strany. 
Příklad: 'Ve smlouvě je uvedeno, že zhotovitel nese vinu za zpoždění.' [[page:5]]
Pokud je coverage 'none', stručně vysvětli proč (např. text o tom nemluví nebo bylo riziko výslovně vyloučeno) a stranu už neuváděj.";
                    var messages = new List<ChatMessage>
                    {
                        new SystemChatMessage(systemPrompt),
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

                    // VÝPOČET SKÓRE PRO TOTO RIZIKO A PŘIČTENÍ DO CELKOVÉHO
                    float multiplier = 0.0f;
                    if (aiResult.Coverage == "full") multiplier = 1.0f;
                    else if (aiResult.Coverage == "partial") multiplier = 0.5f;

                    float riskScore = risk.RiskWeight * multiplier;
                    calculatedTotalScore += riskScore;
                }

                // DOKONČENÍ ANALÝZY A ULOŽENÍ VÝSLEDNÉHO SKÓRE
                string sqlUpdateStatusAndScore = "UPDATE documents SET status = 'done', total_risk_score = @score WHERE id = @docId";
                using (var cmdUpdate = new SqlCommand(sqlUpdateStatusAndScore, conn))
                {
                    cmdUpdate.Parameters.AddWithValue("@docId", docId);
                    cmdUpdate.Parameters.AddWithValue("@score", calculatedTotalScore);
                    await cmdUpdate.ExecuteNonQueryAsync();
                }

                _logger.LogInformation($"Hotovo! Rizika pro dokument {docId} byla vyhodnocena. Celkové skóre: {calculatedTotalScore}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Chyba při vyhodnocování rizik dokumentu: {ex.Message}");
            }
        }
    }
}