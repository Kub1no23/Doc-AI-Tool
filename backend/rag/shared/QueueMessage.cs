using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using System.Text;
using System.Text.Json;

namespace rag.shared
{
    public enum QueueMessageType
    {
        EmbeddingRequest = 1,
        SimilarityRequest = 2,
        DocumentAnalysisCompleted = 3,
        PdfImageExtraction = 4,
        CleanupTask = 5
    }
    public class QueueEnvelope<T>
    {
        public QueueMessageType Type { get; set; }
        public T Payload { get; set; }
    }
    public class EmbedReqPayload
    {
        public Guid DocumentId { get; set; }
        public JsonElement DiResult { get; set; }
    }
    public class SimilarityReqPayload
    {
        public Guid DocumentId { get; set; }
    }

    internal class QueueMessageHelper
    {
        private static readonly JsonSerializerOptions _options = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        public static string Serialize<T>(T obj)
        {
            string json = JsonSerializer.Serialize(obj, _options);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            return Convert.ToBase64String(bytes);
        }

        public static T? Deserialize<T>(string json)
        {
            return JsonSerializer.Deserialize<T>(json, _options);
        }
    }
    public static class QueueSender
    {
        private static readonly string _queueName = "ragqueue";
        public static async Task SendToQueueAsync<T>(QueueMessageType type, T payload)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            var queueClient = new QueueClient(connectionString, _queueName);
            await queueClient.CreateIfNotExistsAsync();

            var envelope = new QueueEnvelope<T>
            {
                Type = type,
                Payload = payload
            };

            string json = QueueMessageHelper.Serialize(envelope);

            await queueClient.SendMessageAsync(json);
        }
    }
}
