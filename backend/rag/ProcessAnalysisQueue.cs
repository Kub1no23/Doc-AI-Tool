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


        //po ziskani zpravy z ai-analysis-queue s docID, se zprava preda do queueMessage
        [Function(nameof(ProcessAnalysisQueue))]
        public async Task Run([QueueTrigger("llm-overview-queue", Connection = "MyDataStorage")] string queueMessage)
        {
            //QueueTrigger - Pristane zprava ve fronte ai-analysis-queue - spust tohle a obas dej do varuable queueMessage
            try
            {
                var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
                string jsonToParse = queueMessage;

                // Kontrola Base64 - pokud to nezacina zavorkou - je to Base64(zpravy ve frontach se historicky posilali a je to dost hnusne akodovane) -> musime dekodovat na json
                if (!queueMessage.Trim().StartsWith("{"))
                {
                    jsonToParse = Encoding.UTF8.GetString(Convert.FromBase64String(queueMessage));
                }


                //changed SimilarityReqPayload for EmbedReqPayload for kuba 
                var envelope = JsonSerializer.Deserialize<QueueEnvelope<EmbedReqPayload>>(jsonToParse, options);
                //Deserialize - Json text z fronty do třídy QueueEnvelope
                if (envelope == null || envelope.Type != QueueMessageType.SimilarityRequest)
                {
                    _logger.LogWarning("Zpráva ignorována: Není typu SimilarityRequest.");
                    return;
                }

                Guid docId = envelope.Payload.DocumentId;
                _logger.LogInformation($"Začínám AI analýzu pro soubor '{envelope.Payload.FileName}' v projektu '{envelope.Payload.Prefix}'.");

                using var conn = new SqlConnection(_sqlConnection);
                await conn.OpenAsync();



                //Idempotence - kdyby napr funkce spadla, mohlo by se pustit znovu a byt duplicity 
                string sqlClearOld = "DELETE FROM risk_analysis_results WHERE document_id = @docId";
                using (var cmdClear = new SqlCommand(sqlClearOld, conn))
                {
                    cmdClear.Parameters.AddWithValue("@docId", docId);
                    await cmdClear.ExecuteNonQueryAsync();
                }

                string sqlResetScore = "UPDATE documents SET total_risk_score = 0 WHERE id = @docId";
                using (var cmdReset = new SqlCommand(sqlResetScore, conn))
                {
                    cmdReset.Parameters.AddWithValue("@docId", docId);
                    await cmdReset.ExecuteNonQueryAsync();
                }



                // nactnei rizik z db 
                //embedding - 1536 cisel v byte - math vyznam rizika
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
                            reader.GetInt32(3), 
                            (byte[])reader["embedding"]
                        ));
                    }
                }

                _logger.LogInformation($"Načetl jsem {risks.Count} rizik k prověření.");


                var openAiClient = new AzureOpenAIClient(new Uri(_openAiEndpoint), new System.ClientModel.ApiKeyCredential(_openAiKey));
                var chatClient = openAiClient.GetChatClient("gpt-4o-mini");

                float calculatedTotalScore = 0.0f;

                //Vector search - sql queries pro kazde riziko a poslani do OpenAI

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
                            //ulozi podobne odstavce do relevandChunks - kazdy z nich ma ChunkId, text a page num
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
                    //text slepen do combinedChunks

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


                    //mantinely pro AI - structured outputs - striktni json schemata s presnymi vlastnostmi
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


                    //posle odstavce a pravidla do OpenAI, translate JSON do objektu RIskAnalysisResult
                    //OpenAI vraci jen json s coverage a explanation - prikazane v jsonSchema a udela z nich RiskAnalysisResult
                    var chatResponse = await chatClient.CompleteChatAsync(messages, chatOptions);
                    var aiResult = JsonSerializer.Deserialize<RiskAnalysisResult>(chatResponse.Value.Content[0].Text, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

                    //vezne GUIDs odstavců(chunkId) nalezene pres vector search, udela z nich json pole, slouzi videni proc AI rozhodla jak rozhodla
                    string chunkIdsJson = JsonSerializer.Serialize(relevantChunks.Select(c => c.ChunkId));

                    //
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



                    // výpočet skóre
                    float multiplier = 0.0f;
                    if (aiResult.Coverage == "full") multiplier = 1.0f;
                    else if (aiResult.Coverage == "partial") multiplier = 0.5f;

                    float riskScore = risk.RiskWeight * multiplier;
                    calculatedTotalScore += riskScore;
                }

                //finish analýzy a uložení výsledného skóre
                string sqlUpdateStatusAndScore = "UPDATE documents SET status = 'done', total_risk_score = @score WHERE id = @docId";
                using (var cmdUpdate = new SqlCommand(sqlUpdateStatusAndScore, conn))
                {
                    cmdUpdate.Parameters.AddWithValue("@docId", docId);
                    cmdUpdate.Parameters.AddWithValue("@score", calculatedTotalScore);
                    await cmdUpdate.ExecuteNonQueryAsync();
                }


                //fan in - kontrola dokonceni projektu
                string sqlGetAnalysisId = "SELECT analysis_id FROM documents WHERE id = @docId";
                Guid analysisId;
                using (var cmdGetId = new SqlCommand(sqlGetAnalysisId, conn))
                {
                    cmdGetId.Parameters.AddWithValue("@docId", docId);
                    analysisId = (Guid)await cmdGetId.ExecuteScalarAsync();
                }

                // 2. Zeptáme se databáze, kolik dokumentů v tomto projektu ještě pracuje
                string sqlCheckPending = "SELECT COUNT(*) FROM documents WHERE analysis_id = @analysisId AND status NOT IN ('done', 'error')";
                int pendingCount;
                using (var cmdCheck = new SqlCommand(sqlCheckPending, conn))
                {
                    cmdCheck.Parameters.AddWithValue("@analysisId", analysisId);
                    pendingCount = (int)await cmdCheck.ExecuteScalarAsync();
                }

                // 3. Pokud jsme poslední, project done v tabulce analysis
                if (pendingCount == 0)
                {
                    _logger.LogInformation($"Všechny dokumenty v projektu {analysisId} jsou hotovy! Uzavírám analýzu.");
                    string sqlCloseAnalysis = "UPDATE analysis SET status = 'done' WHERE id = @analysisId";
                    using (var cmdClose = new SqlCommand(sqlCloseAnalysis, conn))
                    {
                        cmdClose.Parameters.AddWithValue("@analysisId", analysisId);
                        await cmdClose.ExecuteNonQueryAsync();
                    }
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