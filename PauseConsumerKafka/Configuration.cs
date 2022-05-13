namespace PauseConsumerKafka;

public class Configuration
{
    public static string TopicA = "TopicA";

    public static string TopicB = "TopicB";

    public static string Host = "tcp://localhost:8888";

    public const string PauseFlag = "pause";

    public const string ReleaseFlag = "release";

    public static ConsumerConfig ConsumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "Consume_" + TopicB,
        EnableAutoCommit = false
    };
    public static PushSocket Socket = new PushSocket();

    public static CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();
}
