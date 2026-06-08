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
    public class GetDocuments
    {
        private readonly ILogger<GetDocuments> _logger;
        private readonly string _sqlConnection;

        public GetDocuments(ILogger<GetDocuments> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");
        }

        [Function("GetDocuments")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
        {
            string? prefix = req.Query["prefix"];
            if (string.IsNullOrWhiteSpace(prefix))
            {
                return new BadRequestObjectResult(new { error = "Chybí parametr 'prefix' (název projektu)." });
            }

            var documents = new List<object>();

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

    // /SQL dotaz rozšířen o vytáhnutí data(created_at)
            string sql = @"
                SELECT d.id, d.file_name, d.status, d.total_risk_score, d.pdf_url, d.created_at
                FROM documents d
                JOIN analysis a ON d.analysis_id = a.id
                WHERE a.name = @prefix
                ORDER BY d.created_at ASC";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@prefix", prefix);

            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                documents.Add(new
                {
                    documentId = reader.GetGuid(0),
                    fileName = reader.GetString(1),
                    status = reader.GetString(2),
                    totalRiskScore = reader.GetDouble(3),
                    pdfUrl = reader.GetString(4),
                    createdAt = reader.GetDateTime(5) // Přidáno: Čas nahrání dokumentu
                });
            }

            return new OkObjectResult(documents);
        }
    }
}