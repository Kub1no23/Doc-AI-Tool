using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Queues;

namespace rag.shared
{
    public enum QueueMessageType
    {
        DocAIRequest = 1,
        EmbeddingRequest = 2,
        SimilarityRequest = 3,
        SummaryRequest = 4 // <--- NOVÉ
    }

    public class QueueEnvelope<T>
    {
        public QueueMessageType Type { get; set; }
        public T Payload { get; set; }  //data
    }


    public class DocAIReqPayload
    {
        public string Prefix { get; set; }
    }
    public class EmbedReqPayload
    {
        public Guid DocumentId { get; set; }
        public string Prefix { get; set; }
        public string FileName { get; set; }
    }

    //new
    public class SummaryReqPayload
    {
        public Guid AnalysisId { get; set; }
        public string Prefix { get; set; }
    }

    internal class QueueMessageHelper
    {
        private static readonly JsonSerializerOptions _options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        public static string Serialize<T>(T obj) => Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(obj, _options)));
        public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, _options);
    }

    public static class QueueSender
    {
        public static async Task SendToQueueAsync<T>(QueueMessageType type, T payload, double delaySec = 0)
        {
            string queueName = type switch
            {
                QueueMessageType.DocAIRequest => "pdf-json-queue",
                QueueMessageType.EmbeddingRequest => "pdf-embedding-queue",
                QueueMessageType.SimilarityRequest => "llm-overview-queue",
                QueueMessageType.SummaryRequest => "summary-queue", // <--- NOVÉ
                _ => throw new ArgumentOutOfRangeException("Neznámý typ fronty! Zapomněl jsi ho přidat do mapování.")
            };

            var queueClient = new QueueClient(Environment.GetEnvironmentVariable("MyDataStorage"), queueName);
            await queueClient.CreateIfNotExistsAsync();

            await queueClient.SendMessageAsync(
                QueueMessageHelper.Serialize(new QueueEnvelope<T> { Type = type, Payload = payload }),
                visibilityTimeout: TimeSpan.FromSeconds(delaySec)
            );
        }
    }
}