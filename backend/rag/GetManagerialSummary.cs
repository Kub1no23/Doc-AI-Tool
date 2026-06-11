using Azure;
using Azure.AI.OpenAI;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace rag
{
    public class GetManagerialSummary
    {
        private readonly ILogger<GetManagerialSummary> _logger;
        private readonly string _sqlConnection;
        private readonly string _openAiEndpoint;
        private readonly string _openAiKey;

        public GetManagerialSummary(ILogger<GetManagerialSummary> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
            _openAiEndpoint = Environment.GetEnvironmentVariable("OpenAI__Endpoint") ?? throw new Exception("Chybí OpenAI__Endpoint");
            _openAiKey = Environment.GetEnvironmentVariable("OpenAI__ApiKey") ?? throw new Exception("Chybí OpenAI__ApiKey");
        }

        //zmenil jsem route a odebral kod ktery checkoval ID, ted to dela azure sam
        [Function("GetManagerialSummary")]
        //změna AuthorizationLevel.Function na Anonymous - kvůli přístupu, CORS, v praxi JWT, lepsi nez default key pro delani FE a security
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "analyses/{prefix}/summary")] HttpRequest req, string prefix)
        {

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            // 1. Zjistíme, jestli už shrnutí v databázi máme
            string sqlCheck = "SELECT id, final_synthesis_markdown FROM analysis WHERE name = @prefix";
            Guid analysisId = Guid.Empty;
            string? existingSummary = null;

            using (var cmdCheck = new SqlCommand(sqlCheck, conn))
            {
                cmdCheck.Parameters.AddWithValue("@prefix", prefix);
                using var reader = await cmdCheck.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    analysisId = reader.GetGuid(0);
                    if (!reader.IsDBNull(1)) existingSummary = reader.GetString(1);
                }
            }

            if (analysisId == Guid.Empty) return new NotFoundObjectResult(new { error = "Projekt nenalezen." });


            // Pokud už je hotové z minula, rovnou ho vrátíme (šetříme peníze a API volání)
            if (!string.IsNullOrEmpty(existingSummary))
            {
                return new OkObjectResult(new { summary = existingSummary });
            }

            // 2. Pokud není, vytáhneme výsledky všech dokumentů z databáze
            string sqlDocs = "SELECT file_name, total_risk_score FROM documents WHERE analysis_id = @analysisId ORDER BY total_risk_score ASC";
            var docResults = new StringBuilder();

            using (var cmdDocs = new SqlCommand(sqlDocs, conn))
            {
                // TADY CHYBĚL TENTO ŘÁDEK:
                cmdDocs.Parameters.AddWithValue("@analysisId", analysisId);

                using (var reader = await cmdDocs.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        docResults.AppendLine($"- Smlouva: {reader.GetString(0)} | Rizikové skóre: {reader.GetDouble(1)} bodů");
                    }
                }
            }

            if (docResults.Length == 0) return new BadRequestObjectResult(new { error = "V projektu nejsou žádné zpracované dokumenty." });

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
                new UserChatMessage($"Zde jsou konečné výsledky tendru '{prefix}':\n\n{docResults}")
            };

            var chatResponse = await chatClient.CompleteChatAsync(messages);
            string generatedMarkdown = chatResponse.Value.Content[0].Text;

            // 4. Uložíme do databáze, ať už to nikdy nemusíme generovat znovu
            string sqlUpdate = "UPDATE analysis SET final_synthesis_markdown = @md WHERE id = @analysisId";
            using (var cmdUpdate = new SqlCommand(sqlUpdate, conn))
            {
                cmdUpdate.Parameters.AddWithValue("@md", generatedMarkdown);
                cmdUpdate.Parameters.AddWithValue("@analysisId", analysisId);
                await cmdUpdate.ExecuteNonQueryAsync();
            }

            return new OkObjectResult(new { summary = generatedMarkdown });
        }
    }
}