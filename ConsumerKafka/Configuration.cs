namespace ConsumerKafka;
public class Configuration
{
    public static string TopicA = "TopicA";

    public static string Host = "tcp://localhost:8888";

    public const string PauseFlag = "pause";

    public const string ReleaseFlag = "release";

    public static ConsumerConfig ConsumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "Consume_" + TopicA,
        EnableAutoCommit = false
    };

    public static PullSocket Socket = new PullSocket();

    public static CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();

    public static bool HasAlreadyConsumerRunned = false;
}