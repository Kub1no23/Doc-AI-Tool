using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace rag
{
    public class UploadDocument
    {
        private readonly ILogger<UploadDocument> _logger;
        private readonly string _blobConnectionString;

        public UploadDocument(ILogger<UploadDocument> logger)
        {
            _logger = logger;
            // Použijeme stejný connection string jako máme pro Queue (frontu)
            _blobConnectionString = Environment.GetEnvironmentVariable("MyDataStorage") ?? throw new Exception("Chybí MyDataStorage");
        }

        [Function("UploadDocument")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
        {
            string? prefix = req.Query["prefix"];
            if (string.IsNullOrWhiteSpace(prefix))
            {
                return new BadRequestObjectResult(new { error = "Chybí parametr 'prefix' (název projektu)." });
            }

            // Kontrola, zda požadavek obsahuje soubor (multipart/form-data)
            if (!req.HasFormContentType || req.Form.Files.Count == 0)
            {
                return new BadRequestObjectResult(new { error = "Nebyl nalezen žádný soubor k nahrání." });
            }

            var file = req.Form.Files[0]; // Vezmeme první nahraný soubor
            if (file.Length == 0)
            {
                return new BadRequestObjectResult(new { error = "Nahraný soubor je prázdný." });
            }

            try
            {
                // Připojíme se do Blob Storage
                var blobServiceClient = new BlobServiceClient(_blobConnectionString);
                var containerClient = blobServiceClient.GetBlobContainerClient("pdfs");
                await containerClient.CreateIfNotExistsAsync(); // Vytvoří kontejner 'pdfs', pokud náhodou chybí

                // Vytvoříme cestu - např: tendr-hala/nabidka_strabag.pdf
                string blobName = $"{prefix}/{file.FileName}";
                var blobClient = containerClient.GetBlobClient(blobName);

                // Nahrajeme soubor
                using var stream = file.OpenReadStream();
                await blobClient.UploadAsync(stream, overwrite: true);

                _logger.LogInformation($"Soubor {file.FileName} úspěšně nahrán do {blobName}");

                return new OkObjectResult(new
                {
                    message = "Soubor byl úspěšně nahrán do cloudu.",
                    fileName = file.FileName,
                    blobPath = blobName
                });
            }
            catch (Exception ex)
            {
                _logger.LogError($"Chyba při nahrávání souboru: {ex.Message}");
                return new StatusCodeResult(500);
            }
        }
    }
}