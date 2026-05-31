using Microsoft.Azure.Functions.Worker;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace rag
{
    public class GetAnalysisResults
    {
        private readonly ILogger<GetAnalysisResults> _logger;
        private readonly string _sqlConnection;

        public GetAnalysisResults(ILogger<GetAnalysisResults> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection")
                ?? throw new Exception("Chybí SqlConnection");
        }

        [Function("GetAnalysisResults")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "results/{docId}")] HttpRequest req,
            string docId)
        {
            _logger.LogInformation($"Frontend stahuje výsledky pro dokument: {docId}");

            if (!Guid.TryParse(docId, out Guid documentGuid))
            {
                return new BadRequestObjectResult("Neplatné ID dokumentu.");
            }

            var results = new List<object>();

            using (var conn = new SqlConnection(_sqlConnection))
            {
                await conn.OpenAsync();

                // Vytáhneme výsledky, které předtím vytvořil Právník (AI)
                string sql = @"
    SELECT rv.risk_code, r.coverage, r.explanation 
    FROM risk_analysis_results r 
    JOIN risk_vectors rv ON r.risk_id = rv.id 
    WHERE r.document_id = @docId";

                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@docId", documentGuid);

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            results.Add(new
                            {
                                // Použijeme GetValue().ToString(), což bezpečně převede Guid, text i čísla na string
                                riskCode = reader.GetValue(0).ToString(),
                                coverage = reader.GetValue(1).ToString(),
                                explanation = reader.GetValue(2).ToString()
                            });
                        }
                    }
                }
            }

            if (results.Count == 0)
            {
                return new NotFoundObjectResult(new { message = "Analýza ještě probíhá nebo dokument neexistuje." });
            }

            // Výsledek se automaticky převede do JSONu
            return new OkObjectResult(results);
        }
    }
}