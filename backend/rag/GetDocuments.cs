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
            // Načtení connection stringu
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

            // SQL dotaz, který najde všechny dokumenty pro daný název analýzy (prefix)
            // Řadíme podle toho, jak byly vytvořeny (od nejstaršího po nejnovější)
            string sql = @"
                SELECT d.id, d.file_name, d.status 
                FROM documents d
                JOIN analysis a ON d.analysis_id = a.id
                WHERE a.name = @prefix
                ORDER BY d.created_at ASC";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@prefix", prefix);

            using var reader = await cmd.ExecuteReaderAsync();

            // Přečteme řádky z DB a poskládáme JSON objekty
            while (await reader.ReadAsync())
            {
                documents.Add(new
                {
                    documentId = reader.GetGuid(0),
                    fileName = reader.GetString(1),
                    status = reader.GetString(2)
                });
            }

            // Vrátíme seznam (i kdyby byl prázdný, FE dostane prázdné pole [], což je lepší než házet chybu)
            return new OkObjectResult(documents);
        }
    }
}