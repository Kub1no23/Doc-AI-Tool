using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Queues;

namespace rag.shared
{
    // Definice všech možných typů zpráv ve frontě
    public enum QueueMessageType
    {
        EmbeddingRequest = 1,
        SimilarityRequest = 2,
        DocumentAnalysisCompleted = 3,
        PdfImageExtraction = 4,
        CleanupTask = 5
    }

    // Univerzální obálka (přepravka) pro zprávy
    public class QueueEnvelope<T>
    {
        public QueueMessageType Type { get; set; }
        public T Payload { get; set; }  //data
    }

    // Speciální payloady (obsahy zpráv) pro jednotlivé typy úkolů
    // UPRAVENO: Vyhodili jsme obří JsonElement a přidali jméno souboru
    public class EmbedReqPayload
    {
        public Guid DocumentId { get; set; }
        public string Prefix { get; set; }
        public string FileName { get; set; }
    }

    public class SimilarityReqPayload
    {
        public Guid DocumentId { get; set; }
    }

    // Pomocná třída pro balení a rozbalování zpráv (Base64)
    internal class QueueMessageHelper
    {
        private static readonly JsonSerializerOptions _options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        public static string Serialize<T>(T obj) => Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(obj, _options)));
        public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, _options);
    }

    // Pomocná třída pro snadné odesílání zpráv
    public static class QueueSender
    {
        public static async Task SendToQueueAsync<T>(QueueMessageType type, T payload)
        {
            // 1. ZDE JE ZMĚNA: Chytré zjištění správného názvu fronty podle typu zprávy
            string queueName = type switch
            {
                QueueMessageType.EmbeddingRequest => "pdf-embedding-queue", // Pás 1 pro kolegu
                QueueMessageType.SimilarityRequest => "ai-analysis-queue",  // Pás 2 pro tvoji AI
                _ => "ragqueue-default" // Záložní fronta pro ostatní/staré typy
            };

            // 2. Vytvoření klienta rovnou pro tu správnou frontu
            var queueClient = new QueueClient(Environment.GetEnvironmentVariable("MyDataStorage"), queueName);
            await queueClient.CreateIfNotExistsAsync();

            // 3. Odeslání Base64 obálky
            await queueClient.SendMessageAsync(QueueMessageHelper.Serialize(new QueueEnvelope<T> { Type = type, Payload = payload }));
        }
    }
}