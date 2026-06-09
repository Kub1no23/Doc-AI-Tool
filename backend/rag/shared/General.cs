using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rag.shared
{
    internal static class General
    {
        public static async Task<bool> PrefixExistsInDatabaseAsync(string sqlString, string prefix)
        {
            using (var conn = new SqlConnection(sqlString))
            {
                await conn.OpenAsync();

                using (var cmd = new SqlCommand("SELECT COUNT(*) FROM analysis WHERE name = @p", conn))
                {
                    cmd.Parameters.AddWithValue("@p", prefix);

                    int count = (int)await cmd.ExecuteScalarAsync();
                    return count > 0;
                }
            }
        }

        public static async Task UpdateDocumentStatusAsync(string sqlString, Guid id, string status)
        {
            using var conn = new SqlConnection(sqlString);

            await conn.OpenAsync();

            using var cmd = new SqlCommand("UPDATE documents SET status = @status WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@status", status);

            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task UpdateAnalysisStatusAsync(string sqlString, string prefix, string status)
        {
            using var conn = new SqlConnection(sqlString);

            await conn.OpenAsync();

            using var cmd = new SqlCommand("UPDATE analysis SET status = @status WHERE name = @p", conn);

            cmd.Parameters.AddWithValue("@p", prefix);
            cmd.Parameters.AddWithValue("@status", status);

            await cmd.ExecuteNonQueryAsync();
        }
    }
}
