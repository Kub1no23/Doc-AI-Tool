using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace rag
{
    public class GetComparison
    {
        private readonly ILogger<GetComparison> _logger;
        private readonly string _sqlConnection;

        public GetComparison(ILogger<GetComparison> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
        }
        //zmenil jsem route a odebral kod ktery checkoval ID, ted to dela azure sam
        [Function("GetComparison")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "analyses/{prefix}/comparison")] HttpRequest req, string prefix)
        {
            var rankedDocuments = new List<object>();

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            // Vybereme všechny dokumenty pro daný projekt a seřadíme je podle skóre
           
            string sql = @"
                SELECT d.id, d.file_name, d.status, d.total_risk_score 
                FROM documents d
                JOIN analysis a ON d.analysis_id = a.id
                WHERE a.name = @prefix
                ORDER BY d.total_risk_score ASC";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@prefix", prefix);

            using var reader = await cmd.ExecuteReaderAsync();
            int rank = 1;

            while (await reader.ReadAsync())
            {
                rankedDocuments.Add(new
                {
                    rank = rank, // Pořadí v žebříčku
                    documentId = reader.GetGuid(0),
                    fileName = reader.GetString(1),
                    status = reader.GetString(2),
                    totalRiskScore = reader.GetDouble(3)
                });
                rank++;
            }

            if (rankedDocuments.Count == 0)
            {
                return new NotFoundObjectResult(new { message = $"Projekt '{prefix}' neexistuje nebo nemá žádné dokumenty." });
            }

            return new OkObjectResult(new
            {
                project = prefix,
                leaderboard = rankedDocuments
            });
        }
    }
}