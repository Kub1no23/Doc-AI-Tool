using Azure.AI.OpenAI;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OpenAI.Embeddings; // PŘIDÁNO: Potřebné pro EmbeddingGenerationOptions
using System;
using System.ClientModel;
using System.Collections.Generic;
using System.Linq; // Potřebujeme pro .ToArray()
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace rag
{
    public class SeedRisks
    {
        private readonly ILogger<SeedRisks> _logger;
        private readonly string _sqlConnection;
        private readonly string _openAiEndpoint;
        private readonly string _openAiKey;

        public SeedRisks(ILogger<SeedRisks> logger)
        {
            _logger = logger;
            _sqlConnection = Environment.GetEnvironmentVariable("SqlConnection") ?? throw new Exception("Chybí SqlConnection");

            // OPRAVA: Dvě podtržítka!
            _openAiEndpoint = Environment.GetEnvironmentVariable("OpenAI__Endpoint") ?? throw new Exception("Chybí OpenAI__Endpoint");
            _openAiKey = Environment.GetEnvironmentVariable("OpenAI__ApiKey") ?? throw new Exception("Chybí OpenAI__ApiKey");
        }

        [Function("SeedRisks")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "seed-risks")] HttpRequest req)
        {
            _logger.LogInformation("Začínám plnit databázi reálnými stavebními riziky z CSV...");

            var risks = new List<(string Code, string Description, int Weight)>
            {
                ("R001", "Geotechnické riziko: Riziko spojené s nestabilním podložím, sesuvy půdy, vysokou hladinou podzemní vody nebo neočekávanými geologickými podmínkami.", 1),
                ("R002", "Povětrnostní riziko: Riziko zpoždění nebo přerušení prací kvůli nepříznivému počasí - extrémní teploty, srážky, vítr, sněhová pokrývka.", 2),
                ("R003", "Riziko změn v projektu: Riziko změn v projektu během realizace - změny v dokumentaci, dodatečné požadavky zadavatele, změny v rozsahu prací.", 1),
                ("R004", "Riziko nedostatečné dokumentace: Riziko neúplné, nepřesné nebo chybějící projektové dokumentace, která může vést k chybám v realizaci.", 1),
                ("R005", "Riziko cenových změn materiálů: Riziko výrazného zvýšení cen stavebních materiálů během realizace projektu, které není pokryto smlouvou.", 2),
                ("R006", "Riziko nedostupnosti materiálů: Riziko nedostupnosti nebo zpoždění dodávek stavebních materiálů, komponentů nebo zařízení.", 2),
                ("R007", "Riziko zpoždění dodávek: Riziko zpoždění v dodávkách materiálů, zařízení nebo služeb od subdodavatelů, které může ovlivnit harmonogram projektu.", 2),
                ("R010", "Riziko bezpečnosti práce: Riziko pracovních úrazů, havárií nebo porušení bezpečnostních předpisů na stavbě.", 1),
                ("R011", "Riziko ekologických škod: Riziko poškození životního prostředí během realizace - kontaminace půdy, vody, emise, hluk, prach.", 2),
                ("R012", "Riziko střetu s inženýrskými sítěmi: Riziko poškození nebo střetu s existujícími inženýrskými sítěmi (vodovod, kanalizace, elektřina, plyn, telekomunikace).", 1),
                ("R013", "Riziko nedostatečných záruk: Riziko nedostatečných nebo nevhodných záruk za provedené práce, materiály nebo zařízení.", 2),
                ("R014", "Riziko platebních podmínek: Riziko nevýhodných platebních podmínek - dlouhé platební lhůty, zálohy, retence, podmínky fakturace.", 1),
                ("R015", "Riziko smluvních pokut: Riziko vysokých smluvních pokut za nedodržení smluvních podmínek, termínů nebo kvality.", 1),
                ("R016", "Riziko nedostatečného pojištění: Riziko nedostatečného nebo chybějícího pojištění - odpovědnostní pojištění, pojištění stavby, pojištění subdodavatelů.", 2),
                ("R017", "Riziko stavebního řízení: Riziko zpoždění nebo komplikací v získání stavebního povolení, souhlasů nebo dalších úředních rozhodnutí.", 1),
                ("R018", "Riziko sousedských vztahů: Riziko stížností nebo právních sporů se sousedy kvůli stavebním pracím - hluk, prach, vibrace, omezení přístupu. ", 2),
                ("R019", "Riziko nedostatečných kapacit: Riziko nedostatečných kapacit dodavatele - personál, technika, současné zakázky, které může ovlivnit realizaci.", 1),
                ("R020", "Riziko změn legislativy: Riziko změn v legislativě během realizace projektu, které může ovlivnit požadavky, normy nebo podmínky realizace.", 2)
            };

            var openAiClient = new AzureOpenAIClient(new Uri(_openAiEndpoint), new ApiKeyCredential(_openAiKey));
            var embeddingClient = openAiClient.GetEmbeddingClient("text-embedding-3-large");

            using var conn = new SqlConnection(_sqlConnection);
            await conn.OpenAsync();

            _logger.LogInformation("Pročišťuji databázi od starých rizik...");
            using (var cmdDelete = new SqlCommand("DELETE FROM risk_vectors", conn))
            {
                await cmdDelete.ExecuteNonQueryAsync();
            }

            int pocetVlozenych = 0;

            foreach (var risk in risks)
            {
                _logger.LogInformation($"Získávám AI vektor pro riziko: {risk.Code}");

                var embeddingOptions = new EmbeddingGenerationOptions { Dimensions = 1536 };
                var response = await embeddingClient.GenerateEmbeddingAsync(risk.Description, embeddingOptions);
                float[] embeddingVector = response.Value.ToFloats().ToArray();

                // OPRAVA: Převedeme pole na JSON text a ten uložíme jako UTF-8 bajty
                string jsonEmbedding = JsonSerializer.Serialize(embeddingVector);
                byte[] jsonBytes = Encoding.UTF8.GetBytes(jsonEmbedding);

                string sql = @"
                    INSERT INTO risk_vectors (risk_code, text, risk_weight, embedding)
                    VALUES (@code, @text, @weight, @embedding)";

                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@code", risk.Code);
                cmd.Parameters.AddWithValue("@text", risk.Description);
                cmd.Parameters.AddWithValue("@weight", risk.Weight);
                cmd.Parameters.AddWithValue("@embedding", jsonBytes);

                await cmd.ExecuteNonQueryAsync();
                pocetVlozenych++;
            }

            return new OkObjectResult($"Úspěch! Do databáze bylo vloženo a ovektorováno {pocetVlozenych} reálných stavebních rizik s délkou 1536 dimenzí.");
        }
    }
}