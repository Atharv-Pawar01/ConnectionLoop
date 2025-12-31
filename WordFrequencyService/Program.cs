using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;

namespace WordFrequencyService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            
            
            builder.Services.AddHostedService<NatsWorker>();

            var host = builder.Build();
            host.Run();
        }
    }

    public class NatsWorker : BackgroundService
    {
        private readonly ILogger<NatsWorker> _log;
        private const string Url = "nats://localhost:4222"; 
        private const string TargetSubject = "word.frequency";

        public NatsWorker(ILogger<NatsWorker> logger)
        {
            _log = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken token)
        {
            _log.LogInformation("Connecting to NATS server at {Url}...", Url);

            try
            {
                
                var opts = new NatsOpts
                {
                    Url = Url,
                    SerializerRegistry = NatsJsonSerializerRegistry.Default
                };

                
                await using var conn = new NatsConnection(opts);
                
                _log.LogInformation("Ready. Subject: {Sub}", TargetSubject);

                
                await foreach (var msg in conn.SubscribeAsync<InputData>(TargetSubject, cancellationToken: token))
                {
                    if (msg.Data is null) continue;

                    _log.LogInformation("Processing request. Limit: {N}", msg.Data.N);

                    try
                    {
                        
                        var result = AnalyzeText(msg.Data);

                        
                        await msg.ReplyAsync(result, cancellationToken: token);
                        _log.LogInformation("Reply sent. Total words: {Total}", result.Count);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Handler failed");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _log.LogInformation("Service stopping...");
            }
            catch (Exception ex)
            {
                _log.LogCritical(ex, "NATS Loop crashed");
            }
        }

        private OutputData AnalyzeText(InputData input)
        {
            if (string.IsNullOrWhiteSpace(input.Content))
            {
                return new OutputData 
                { 
                    Items = new List<Item>(), 
                    Count = 0 
                };
            }

            var raw = input.Content.ToLowerInvariant();

            
            var list = Regex.Split(raw, @"[\W_]+")
                             .Where(x => !string.IsNullOrWhiteSpace(x))
                             .ToList();

            var total = list.Count;

            int limit = input.N > 0 ? input.N : 3; // default to 3 if zero/negative logic

            var grouped = list
                .GroupBy(x => x)
                .Select(g => new Item 
                { 
                    Key = g.Key, 
                    Val = g.Count() 
                })
                .OrderByDescending(x => x.Val)
                .ThenBy(x => x.Key)
                .Take(limit)
                .ToList();

            return new OutputData
            {
                Items = grouped,
                Count = total
            };
        }
    }

    

    public class InputData
    {
        [JsonPropertyName("text")]
        public string Content { get; set; } = "";

        [JsonPropertyName("topN")]
        public int N { get; set; }
    }

    public class OutputData
    {
        [JsonPropertyName("wordFrequencies")]
        public List<Item> Items { get; set; } = new();

        [JsonPropertyName("totalWords")]
        public int Count { get; set; }
    }

    public class Item
    {
        [JsonPropertyName("word")]
        public string Key { get; set; } = "";

        [JsonPropertyName("count")]
        public int Val { get; set; }
    }
}