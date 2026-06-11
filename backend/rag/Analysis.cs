using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Data.SqlClient;

public class Analysis
{
    private readonly ILogger<Analysis> _logger;
    private readonly string _connectionString;

    public Analysis(ILogger<Analysis> logger)
    {
        _logger = logger;
        _connectionString = Environment.GetEnvironmentVariable("SqlConnection")
            ?? throw new InvalidOperationException("SqlConnection env variable is missing.");
    }

    [Function("CreateAnalysis")]
    //změna AuthorizationLevel.Function na Anonymous - kvůli přístupu, CORS, v praxi JWT, lepsi nez default key pro delani FE a security

    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "analysis")] HttpRequest req)
    {
        string? analysisName = req.Query["name"];
        if (string.IsNullOrWhiteSpace(analysisName))
            return new BadRequestObjectResult("Missing analysis name query parameter.");

        _logger.LogInformation("Creating analysis with name: {Name}", analysisName);

        using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();

            string sql = @"
                INSERT INTO analysis (name)
                OUTPUT INSERTED.id, INSERTED.name, INSERTED.status
                VALUES (@name);
            ";

            try
            {
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@name", analysisName);

                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        return new OkObjectResult(new
                        {
                            id = reader.GetGuid(0),
                            name = reader.GetString(1),
                            status = reader.GetString(2)
                        });
                    }
                };
            }
            catch (SqlException ex) when (ex.Number == 2601 || ex.Number == 2627)
            {
                _logger.LogWarning("Duplicate analysis name: {Name}", analysisName);

                return new ConflictObjectResult(new
                {
                    error = "Analysis with this name already exists.",
                    name = analysisName
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while creating analysis.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }

            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        };
    }

    [Function("GetAllAnalysis")]
    //změna AuthorizationLevel.Function na Anonymous - kvůli přístupu, CORS, v praxi JWT, lepsi nez default key pro delani FE a security

    public async Task<IActionResult> GetAll([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "analysis")] HttpRequest req)
    {
        _logger.LogInformation("Fetching all analysis records.");

        using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();

            string sql = @"
                SELECT 
                    id,
                    name,
                    status,
                    final_synthesis_markdown,
                    created_at,
                    updated_at
                FROM analysis
                ORDER BY created_at DESC;
            ";

            try
            {
                using (var cmd = new SqlCommand(sql, conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var results = new List<object>();

                    while (await reader.ReadAsync())
                    {
                        results.Add(new
                        {
                            id = reader.GetGuid(0),
                            name = reader.GetString(1),
                            status = reader.GetString(2),
                            final_synthesis_markdown = reader.IsDBNull(3) ? null : reader.GetString(3),
                            created_at = reader.GetDateTime(4),
                            updated_at = reader.GetDateTime(5)
                        });
                    }

                    return new OkObjectResult(results);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while fetching analysis list.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }

    [Function("DeleteAnalysis")]
    //změna AuthorizationLevel.Function na Anonymous - kvůli přístupu, CORS, v praxi JWT, lepsi nez default key pro delani FE a security

    public async Task<IActionResult> Delete([HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "analysis/{id}")] HttpRequest req, string id)
    {
        if (!Guid.TryParse(id, out Guid analysisId))
            return new BadRequestObjectResult("Invalid analysis ID.");

        _logger.LogInformation("Deleting analysis with id: {Id}", analysisId);

        using (var conn = new SqlConnection(_connectionString))
        {
            await conn.OpenAsync();

            string sql = @"
                DELETE FROM analysis
                OUTPUT DELETED.id, DELETED.name
                WHERE id = @id;
            ";

            try
            {
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@id", analysisId);

                    using var reader = await cmd.ExecuteReaderAsync();

                    if (await reader.ReadAsync())
                    {
                        return new OkObjectResult(new
                        {
                            success = $"Deleted analysis {reader.GetString(1)}.",
                            id = reader.GetGuid(0)
                        });
                    }
                    else
                    {
                        return new NotFoundObjectResult(new
                        {
                            error = $"Analysis not found.",
                            id = analysisId
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while deleting analysis.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}