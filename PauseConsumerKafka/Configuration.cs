namespace PauseConsumerKafka;

public class Configuration
{
    public static string TopicCash = "TopicCash";

    public static string TopicFlag = "TopicFlag";

    public static string Host = "tcp://localhost:8888";

    public const string PauseFlag = "pause";

    public const string ReleaseFlag = "release";

    public static ConsumerConfig ConsumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "Consume_" + TopicFlag,
        EnableAutoCommit = false
    };
    public static PushSocket Socket = new PushSocket();

    public static CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();
}
