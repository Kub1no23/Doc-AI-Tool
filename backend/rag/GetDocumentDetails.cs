using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Text.Json;

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
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "documents/{documentId:guid}/results")] HttpRequest req, Guid documentId)
        {
            var results = new List<object>();

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            // Přidali jsme ISNULL(rar.matched_pages, '[]') jako 5. sloupec
            string sql = @"
        SELECT 
            rv.risk_code, 
            rv.text, 
            ISNULL(rar.coverage, 'none') AS coverage, 
            ISNULL(rar.explanation, 'AI systém toto riziko nenašel, nebo k němu nevygeneroval popis.') AS explanation,
            ISNULL(rar.matched_pages, '[]') AS matched_pages
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
                string rawExplanation = reader.GetString(3);
                string matchedPagesJson = reader.GetString(4); // Nový sloupec s JSON polem stránek

                // Odstraníme zbytky [[page:X]] tagů, kdyby je tam AI náhodou přece jen dalo (pro jistotu)
                string cleanExplanation = System.Text.RegularExpressions.Regex.Replace(rawExplanation, @"\[\[page:\s*\d+\s*\]\]", "").Trim();

                // Jednoduše deserializujeme naše uložené pole čísel
                List<int> pageNumbers = JsonSerializer.Deserialize<List<int>>(matchedPagesJson) ?? new List<int>();

                results.Add(new
                {
                    riskCode = reader.GetString(0),
                    riskName = reader.GetString(1),
                    coverage = reader.GetString(2),
                    explanation = cleanExplanation,
                    pages = pageNumbers // Bude vždy obsahovat přesná čísla (např. [5, 12])
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