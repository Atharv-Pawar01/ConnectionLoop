using System.Text.Json;
using System.Text.Json.Serialization;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;


var opts = new NatsOpts
{
    Url = "nats://localhost:4222",
    SerializerRegistry = NatsJsonSerializerRegistry.Default
};

await using var conn = new NatsConnection(opts);


var content = "the quick brown fox jumps over the lazy dog the fox";
if (args.Length > 0) 
{
    content = args[0];
}

int limit = 3;
if (args.Length > 1)
{
    int.TryParse(args[1], out limit);
}

var payload = new InputData
{
    Content = content,
    N = limit
};

Console.WriteLine($"Sending: {payload.Content}");
Console.WriteLine($"Limit: {payload.N}");


try 
{
    var msg = await conn.RequestAsync<InputData, OutputData>(
        subject: "word.frequency", 
        data: payload, 
        replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(5) }
    );

    
    if (msg.Data != null)
    {
        Console.WriteLine($"\nTotal: {msg.Data.Count}");
        Console.WriteLine("Top:");
        foreach (var x in msg.Data.Items)
        {
            Console.WriteLine($" - {x.Key}: {x.Val}");
        }
    }
}
catch (NatsNoReplyException)
{
    Console.WriteLine("No reply. Service running?");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
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