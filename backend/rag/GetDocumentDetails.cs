using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace rag
{
    public class GetDocumentDetails
    {
        private readonly ILogger<GetDocumentDetails> _logger;
        private readonly string _sqlConnection;

        public GetDocumentDetails(ILogger<GetDocumentDetails> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
        }

        //zmenil jsem route a odebral kod ktery checkoval ID, ted to dela azure sam
        [Function("GetDocumentDetails")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "documents/{documentId:guid}/risks")] HttpRequest req, Guid documentId)
        {
            var results = new List<object>();

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            // Vytáhneme výsledky analýzy a spojíme je s definicí rizik.
            // Seřadíme je tak, aby 'full' rizika byla nahoře, pak 'partial' a nakonec 'none'.
            string sql = @"
                SELECT 
                    rv.risk_code, 
                    rv.text, 
                    ISNULL(rar.coverage, 'none') AS coverage, 
                    ISNULL(rar.explanation, 'AI systém toto riziko nenašel, nebo k němu nevygeneroval popis.') AS explanation
                FROM risk_vectors rv
                LEFT JOIN risk_analysis_results rar 
                    ON rv.id = rar.risk_id AND rar.document_id = @docId
                ORDER BY 
                    CASE ISNULL(rar.coverage, 'none')
                        WHEN 'full' THEN 1 
                        WHEN 'partial' THEN 2 
                        WHEN 'none' THEN 3 
                        ELSE 4 
                    END ASC";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@docId", documentId);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                // 1. Uložíme si původní hrubou odpověď od AI do proměnné
                string rawExplanation = reader.GetString(3);

                // Připravíme si prázdný seznam pro případ, že AI vrátí více odkazů na stránky
                var pageNumbers = new List<int>();

                // 2. Extrakce všech výskytů tagů stránek (např. [[page:5]])
                var matches = Regex.Matches(rawExplanation, @"\[\[page:\s*(\d+)\s*\]\]");
                foreach (Match match in matches)
                {
                    // Převedeme vytažený text na číslo a přidáme do pole (pokud tam ještě není)
                    if (int.TryParse(match.Groups[1].Value, out int page) && !pageNumbers.Contains(page))
                    {
                        pageNumbers.Add(page);
                    }
                }

                // 3. Odstranění tagů z původního textu a oříznutí zbytečných mezer na koncích
                string cleanExplanation = Regex.Replace(rawExplanation, @"\[\[page:\s*\d+\s*\]\]", "").Trim();

                // 4. Přidání do finálního výsledku pro frontend
                results.Add(new
                {
                    riskCode = reader.GetString(0),
                    riskName = reader.GetString(1),
                    coverage = reader.GetString(2),
                    explanation = cleanExplanation, // Očištěný text
                    pages = pageNumbers             // Samostatné pole intů (např. [5, 7] nebo [])
                });
            }

            if (results.Count == 0)
            {
                return new NotFoundObjectResult(new { message = $"Pro dokument '{documentId}' nebyly nalezeny žádné výsledky." });
            }

            return new OkObjectResult(new
            {
                documentId = documentId,
                risks = results
            });
        }
    }
}