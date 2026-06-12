using Azure.AI.OpenAI;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using rag.shared;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace rag
{
    public class CreateSummary
    {
        private readonly ILogger<CreateSummary> _logger;
        private readonly string _sqlConnection;
        private readonly string _openAiEndpoint;
        private readonly string _openAiKey;

        public CreateSummary(ILogger<CreateSummary> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
            _openAiEndpoint = Environment.GetEnvironmentVariable("OpenAI__Endpoint") ?? throw new Exception("Chybí OpenAI__Endpoint");
            _openAiKey = Environment.GetEnvironmentVariable("OpenAI__ApiKey") ?? throw new Exception("Chybí OpenAI__ApiKey");
        }

        [Function("CreateSummary")]
        // Použití silně typované třídy v parametru (automatická deserializace z fronty)
        public async Task Run([QueueTrigger("summary-queue", Connection = "MyDataStorage")] QueueEnvelope<SummaryReqPayload> message)
        {
            _logger.LogInformation($"Začínám generovat manažerské shrnutí pro projekt: {message.Payload.Prefix}");

            Guid analysisId = message.Payload.AnalysisId;

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            // 1. Zajištění idempotence (pokud už markdown existuje, neděláme to znovu)
            string sqlCheck = "SELECT final_synthesis_markdown FROM analysis WHERE id = @analysisId";
            using (var cmdCheck = new SqlCommand(sqlCheck, conn))
            {
                cmdCheck.Parameters.AddWithValue("@analysisId", analysisId);
                var existing = await cmdCheck.ExecuteScalarAsync();
                if (existing != DBNull.Value && existing != null)
                {
                    _logger.LogInformation("Shrnutí již existuje, přeskakuji generování.");
                    return;
                }
            }

            // 2. Vytáhneme výsledky všech dokumentů z databáze
            string sqlDocs = "SELECT file_name, total_risk_score FROM documents WHERE analysis_id = @analysisId ORDER BY total_risk_score ASC";
            var docResults = new StringBuilder();

            using (var cmdDocs = new SqlCommand(sqlDocs, conn))
            {
                cmdDocs.Parameters.AddWithValue("@analysisId", analysisId);

                using (var reader = await cmdDocs.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        docResults.AppendLine($"- Smlouva: {reader.GetString(0)} | Rizikové skóre: {reader.GetDouble(1)} bodů");
                    }
                }
            }

            if (docResults.Length == 0)
            {
                _logger.LogWarning($"V projektu {analysisId} nejsou žádné dokumenty. Končím.");
                return;
            }

            // 3. Necháme OpenAI napsat manažerské shrnutí
            var openAiClient = new AzureOpenAIClient(new Uri(_openAiEndpoint), new System.ClientModel.ApiKeyCredential(_openAiKey));
            var chatClient = openAiClient.GetChatClient("gpt-4o-mini");

            string systemPrompt = @"Jsi hlavní projektový manažer a auditor. Tvým úkolem je napsat stručné manažerské shrnutí (Executive Summary) pro výběrové řízení. 
Dostaneš seznam hodnocených smluv a jejich rizikové skóre. Pravidlo: Čím NIŽŠÍ skóre, tím BEZPEČNĚJŠÍ smlouva. Skóre blízké nule znamená téměř nulové riziko.
Napiš shrnutí ve formátu Markdown. 
1. Nejprve jasně vyhlás vítěze (nejbezpečnější smlouvu) a stručně doporuč, proč by si ji měl management vybrat.
2. Následně v odrážkách stručně okomentuj VŠECHNY ostatní předložené smlouvy, uveď jejich skóre a hlavní důvod, proč se umístily hůře.
3. Důrazně varuj před nabídkou s nejvyšším skóre. 
Piš profesionálně, sebevědomě a logicky strukturovaně.";

            var messages = new List<ChatMessage>
            {
                new SystemChatMessage(systemPrompt),
                new UserChatMessage($"Zde jsou konečné výsledky tendru '{message.Payload.Prefix}':\n\n{docResults}")
            };

            var chatResponse = await chatClient.CompleteChatAsync(messages);
            string generatedMarkdown = chatResponse.Value.Content[0].Text;

            // 4. Uložíme text do databáze A ZÁROVEŇ nastavíme status celé analýzy na 'done'
            string sqlUpdate = "UPDATE analysis SET final_synthesis_markdown = @md, status = 'done' WHERE id = @analysisId";
            using (var cmdUpdate = new SqlCommand(sqlUpdate, conn))
            {
                cmdUpdate.Parameters.AddWithValue("@md", generatedMarkdown);
                cmdUpdate.Parameters.AddWithValue("@analysisId", analysisId);
                await cmdUpdate.ExecuteNonQueryAsync();
            }

            _logger.LogInformation($"Úspěch! Shrnutí vygenerováno a projekt {message.Payload.Prefix} je kompletně DOKONČEN (status: done).");
        }
    }
}